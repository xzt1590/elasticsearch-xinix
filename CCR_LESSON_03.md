# 第 3 课：Retention Lease（保留租约）

## 一、回顾问题

上节课讲了 Soft Deletes 让历史操作保留在 Lucene 中，但 `SoftDeletesPolicy` 最终会允许 merge 清除过期的 soft-deleted 文档。

问题来了：**谁来告诉 SoftDeletesPolicy "这个 seq_no 之后的别删"？**

答案就是 **Retention Lease**。

## 二、什么是 Retention Lease

看 `RetentionLease.java:25-28` 的注释：

> A "shard history retention lease" (or "retention lease" for short) is conceptually a marker containing a retaining sequence number such that all operations with sequence number at least that retaining sequence number will be retained during merge operations (which could otherwise merge away operations that have been soft deleted).

翻译成人话：一张"保护令"，告诉 Leader 的引擎：**"seq_no >= X 的 soft-deleted 文档你别清理，我还需要。"**

## 三、RetentionLease 的数据结构

```java
public final class RetentionLease {
    private final String id;                    // 唯一标识
    private final long retainingSequenceNumber; // 保留的起始 seq_no
    private final long timestamp;               // 创建/续期时间戳
    private final String source;                // 来源（"ccr" 或 "peer_recovery"）
}
```

四个字段，非常简单：

| 字段                    | 含义                                   | 例子                                                            |
|-------------------------|----------------------------------------|-----------------------------------------------------------------|
| id                      | 租约的唯一标识                         | "my-cluster/follower-idx/uuid-following-remote/leader-idx/uuid" |
| retainingSequenceNumber | "从这个 seq_no 开始往后的操作都要保留" | 50001                                                           |
| timestamp               | 创建或最近一次续期的时间               | 1718000000000                                                   |
| source                  | 谁创建的                               | "ccr" 或 "peer_recovery"                                        |

注意 `source` 字段会做字符串去重节省内存：

```java
this.source = switch (source) {
    case "ccr" -> "ccr";
    case ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE -> ...;
    default -> source;
};
```

## 四、RetentionLeases —— 租约集合

一个 shard 上可能有多个 retention lease（多个 follower 在跟随同一个 leader shard），所以有一个集合类 `RetentionLeases`：

```java
public class RetentionLeases {
    private final long primaryTerm;           // 哪个 primary term 下创建的
    private final long version;               // 每次增/删/续租时 version+1
    private final Map<String, RetentionLease> leases;  // id → lease 的 map
}
```

版本化设计的原因：租约集合需要同步到所有 replica。如果乱序到达，用 `supersedes()` 判断哪个更新：

```java
boolean supersedes(final long primaryTerm, final long version) {
    return this.primaryTerm > primaryTerm || this.primaryTerm == primaryTerm && this.version > version;
}
```

## 五、租约的生命周期

### 5.1 创建（Add）

在 `ReplicationTracker.addRetentionLease()`（第 303 行）中：

```java
public RetentionLease addRetentionLease(String id, long retainingSequenceNumber, String source, ActionListener listener) {
    synchronized (this) {
        retentionLease = innerAddRetentionLease(id, retainingSequenceNumber, source);
        currentRetentionLeases = retentionLeases;  // version + 1
    }
    onSyncRetentionLeases.accept(currentRetentionLeases, listener);  // 同步到 replica
    return retentionLease;
}
```

创建后立刻同步到所有 replica（因为 replica 也需要知道有哪些租约，这影响它本地的 merge 行为）。

### 5.2 续期（Renew）

在 `ReplicationTracker.renewRetentionLease()`（第 391 行）中：

```java
public synchronized RetentionLease renewRetentionLease(String id, long retainingSequenceNumber, String source) {
    // 不允许 retainingSequenceNumber 回退
    if (retainingSequenceNumber < existingRetentionLease.retainingSequenceNumber()) {
        throw new RetentionLeaseInvalidRetainingSeqNoException(...);
    }
    // 用新的 retainingSequenceNumber 和当前时间戳替换旧的
    retentionLease = new RetentionLease(id, retainingSequenceNumber, currentTimeMillis, source);
    // version + 1
}
```

关键规则：**retainingSequenceNumber 只能往前推进，不能回退**。这很合理——follower 处理完的操作不需要再保留。

续期做两件事：
1. 更新 retainingSequenceNumber（我已经处理到这了，之前的可以清了）
2. 更新 timestamp（告诉系统"我还活着"）

### 5.3 过期（Expire）

在 `ReplicationTracker.getRetentionLeases(true)`（第 235 行）中：

```java
// 过期判断
return currentTimeMillis - lease.timestamp() > retentionLeaseMillis;
```

过期时间由配置决定：

```java
// IndexSettings.java:482
"index.soft_deletes.retention_lease.period"  // 默认 12 小时
```

如果一个租约超过 12 小时没有续期，它就会被视为过期并移除。

这意味着：如果 follower 挂了超过 12 小时没续期，它的租约就过期了，Leader 可能会清理掉它需要的历史操作。Follower 恢复后发现数据丢失，只能重新 bootstrap。

### 5.4 删除（Remove）

在 `ReplicationTracker.removeRetentionLease()`（第 428 行）中：

```java
public void removeRetentionLease(String id, ActionListener listener) {
    synchronized (this) {
        retentionLeases = new RetentionLeases(
            operationPrimaryTerm,
            retentionLeases.version() + 1,
            retentionLeases.leases().stream().filter(lease -> !lease.id().equals(id)).toList()
        );
    }
    onSyncRetentionLeases.accept(currentRetentionLeases, listener);  // 同步到 replica
}
```

主动删除，比如 unfollow 时 follower 不再需要保留，会移除租约。

## 六、CCR 如何使用 Retention Lease

`CcrRetentionLeases.java` 封装了 CCR 对租约的所有操作。

### 6.1 租约 ID 的命名规则

```java
public static String retentionLeaseId(...) {
    return String.format(Locale.ROOT,
        "%s/%s/%s-following-%s/%s/%s",
        localClusterName,        // follower 集群名
        followerIndex.getName(), // follower 索引名
        followerIndex.getUUID(), // follower 索引 UUID
        remoteClusterAlias,      // 远程集群别名
        leaderIndex.getName(),   // leader 索引名
        leaderIndex.getUUID()    // leader 索引 UUID
    );
}
```

例如：`"prod-dc2/orders-follower/abc123-following-dc1/orders/def456"`

这保证了每个 follower shard 在 leader 上的租约是唯一的。

### 6.2 在 Leader 上创建租约

```java
public static void asyncAddRetentionLease(ShardId leaderShardId, String retentionLeaseId,
    long retainingSequenceNumber, RemoteClusterClient remoteClient, ActionListener listener) {

    RetentionLeaseActions.AddRequest request = new RetentionLeaseActions.AddRequest(
        leaderShardId, retentionLeaseId, retainingSequenceNumber, "ccr"  // source = "ccr"
    );
    remoteClient.execute(RetentionLeaseActions.REMOTE_ADD, request, listener);
}
```

注意这是通过 `RemoteClusterClient` 在 leader 集群上执行的。租约存在于 leader shard 的 `ReplicationTracker` 中。

### 6.3 续期

```java
public static void asyncRenewRetentionLease(ShardId leaderShardId, String retentionLeaseId,
    long retainingSequenceNumber, RemoteClusterClient remoteClient, ActionListener listener) {
    // source = "ccr"
    remoteClient.execute(RetentionLeaseActions.REMOTE_RENEW, request, listener);
}
```

CCR 默认每 30 秒续期一次：

```java
// CcrRetentionLeases.java:31
RETENTION_LEASE_RENEW_INTERVAL_SETTING = Setting.timeSetting(
    "index.ccr.retention_lease.renew_interval",
    new TimeValue(30, TimeUnit.SECONDS), ...
);
```

## 七、Retention Lease 如何影响 SoftDeletesPolicy

回顾上节课的 `SoftDeletesPolicy.getMinRetainedSeqNo()`：

```java
final long minimumRetainingSequenceNumber = retentionLeases.leases()
    .stream()
    .mapToLong(RetentionLease::retainingSequenceNumber)
    .min()                    // 取所有租约中最小的 retainingSequenceNumber
    .orElse(Long.MAX_VALUE);  // 没有租约就不额外保留
```

所有租约中最小的 `retainingSequenceNumber` 决定了保留下限。

举例：

```
Leader shard 上有三个租约：
  Lease-A (follower-1): retainingSequenceNumber = 5000
  Lease-B (follower-2): retainingSequenceNumber = 8000
  Lease-C (peer-recovery): retainingSequenceNumber = 9000

→ minimumRetainingSequenceNumber = 5000
→ seq_no >= 5000 的 soft-deleted 文档必须保留
→ seq_no < 5000 的可以在 merge 时清除
```

这就是为什么一个落后很多的 follower 会导致 leader 的磁盘/内存压力——它的租约把保留下限拉得很低。

## 八、租约丢失后会怎样

场景：follower 挂了 > 12 小时 → 租约过期 → leader merge 清除了历史操作 → follower 恢复

```
Follower: "给我 seq_no 5000~10000 的操作"
Leader:   "抱歉，seq_no < 8000 的已经被清理了" → 抛出 MissingHistoryOperationsException
Follower: 无法继续增量复制 → 必须重新 bootstrap（从头复制全量数据）
```

这是 CCR 运维中的常见问题。解决方案：
- 确保 follower 不要长时间离线
- 适当调大 `index.soft_deletes.retention_lease.period`
- 监控 `ccr/stats` 中的 follower lag

## 九、两种使用者

Retention Lease 不只是 CCR 用，peer recovery 也用：

| 使用者        | source          | 目的                                |
|---------------|-----------------|-------------------------------------|
| CCR           | "ccr"           | follower 需要读 leader 的历史操作   |
| Peer Recovery | "peer_recovery" | replica 恢复时需要 primary 保留操作 |

peer recovery 的租约有额外的过期逻辑：如果所有 shard 都已 started，那些属于不再存在的节点的 peer recovery 租约会被直接清除。

## 十、完整交互时序

```
开始 follow:
  Follower                                    Leader
     │                                           │
     ├── addRetentionLease(seqNo=0) ───────────→ │  "从 seq_no=0 开始给我保留"
     │                                           │  Leader 的 SoftDeletesPolicy 感知到租约
     │                                           │

每 30 秒:
     │                                           │
     ├── renewRetentionLease(seqNo=5000) ──────→ │  "我已处理到 5000，之前的可以清了"
     │                                           │  Leader 推进 minRetainedSeqNo
     │                                           │
     ├── renewRetentionLease(seqNo=8000) ──────→ │  "我已处理到 8000"
     │                                           │

unfollow 时:
     │                                           │
     ├── removeRetentionLease() ───────────────→ │  "我不需要了，随便清"
     │                                           │
```

## 十一、小结

| 概念                    | 含义                                                                    |
|-------------------------|-------------------------------------------------------------------------|
| RetentionLease          | 一张保护令："seq_no >= X 的别清"                                        |
| retainingSequenceNumber | 保留起点，只能向前推进                                                  |
| timestamp               | 上次续期时间，用于判断是否过期                                          |
| 过期时间                | 默认 12 小时（`index.soft_deletes.retention_lease.period`）             |
| 续期频率                | CCR 默认 30 秒                                                          |
| 存储位置                | Leader shard 的 ReplicationTracker 中                                   |
| 影响                    | SoftDeletesPolicy 取所有租约的最小 retainingSequenceNumber 作为保留下限 |

**关键理解**：Retention Lease 是 follower 在 leader 上持有的，是 follower 告诉 leader "我需要你保留数据"的机制。它是跨集群的通信协议。

---

## 十二、深入：分片之间的租约关系

### 12.1 租约属于谁

租约由 **primary shard 管理**，但会**同步到所有 replica**。

```
Primary shard 的 ReplicationTracker:
  ├── 负责 add / renew / remove / expire 操作（只能在 primaryMode 下执行）
  └── 每次变更后调用 onSyncRetentionLeases → 同步到所有 replica

Replica shard 的 ReplicationTracker:
  └── updateRetentionLeasesOnReplica(retentionLeases) → 被动接收，直接替换
```

代码证据——所有写操作都有 `assert primaryMode`：

```java
public synchronized RetentionLease renewRetentionLease(...) {
    assert primaryMode;  // 只有 primary 能续期
    ...
}
```

Replica 只有被动接收：

```java
public synchronized void updateRetentionLeasesOnReplica(final RetentionLeases retentionLeases) {
    assert primaryMode == false;  // 只有 replica 走这个方法
    if (retentionLeases.supersedes(this.retentionLeases)) {
        this.retentionLeases = retentionLeases;
    }
}
```

### 12.2 为什么 Replica 也需要租约信息

因为 **replica 也会发生 merge**。Merge 时 `SoftDeletesPolicy` 要判断哪些 soft-deleted 文档可以清理，这个判断需要 retention leases 信息。如果 replica 不知道有哪些租约，它可能过早清理 soft-deleted 文档，导致将来这个 replica 被提升为 primary 时缺少历史操作。

### 12.3 为什么一个 Primary Shard 上会有多个租约

一个 primary shard 同时被多个消费者需要保留历史，每个消费者持有自己的租约：

**Peer Recovery Retention Lease（每个副本一个）**

当索引有 1 primary + 2 replica 时：

```
Primary shard 上的租约:
  ├── "peer_recovery/node-A"  retainingSeqNo=9500  ← primary 自己（以防被其他节点恢复）
  ├── "peer_recovery/node-B"  retainingSeqNo=9200  ← replica-1 在 node-B 上
  └── "peer_recovery/node-C"  retainingSeqNo=9000  ← replica-2 在 node-C 上
```

这些 peer recovery 租约的作用：如果 replica 临时挂了再恢复，primary 保留了它需要的操作，可以做基于操作的恢复（ops-based recovery），而不需要拷贝整个 shard 文件（file-based recovery）。

租约 ID 格式：`"peer_recovery/" + nodeId`

由 primary 的 `ReplicationTracker` 自动管理——primary 知道有哪些 replica 分片，为每个自动创建并定期推进：

```java
public RetentionLease addPeerRecoveryRetentionLease(String nodeId, long globalCheckpoint, ...) {
    return addRetentionLease(
        getPeerRecoveryRetentionLeaseId(nodeId),   // "peer_recovery/node-B"
        globalCheckpoint + 1,                       // 保留 gcp 之后的操作
        PEER_RECOVERY_RETENTION_LEASE_SOURCE,       // "peer recovery"
        listener
    );
}
```

**CCR Retention Lease（每个 follower 一个）**

如果有两个不同集群的 follower 在跟随这个 leader shard：

```
Primary shard 上的租约（续上面的）:
  ├── "peer_recovery/node-A"                                    retainingSeqNo=9500
  ├── "peer_recovery/node-B"                                    retainingSeqNo=9200
  ├── "peer_recovery/node-C"                                    retainingSeqNo=9000
  ├── "dc2/orders-follower/uuid1-following-dc1/orders/uuid0"    retainingSeqNo=7000  ← CCR follower-1
  └── "dc3/orders-follower/uuid2-following-dc1/orders/uuid0"    retainingSeqNo=3000  ← CCR follower-2
```

5 个租约，`SoftDeletesPolicy` 取最小值 = 3000。所以 `seq_no >= 3000` 的 soft-deleted 文档都要保留。

### 12.4 Peer Recovery 租约的续期机制

和 CCR 不同，peer recovery 的租约**不需要 replica 主动续期**——primary 自己来推进：

```java
public synchronized void renewPeerRecoveryRetentionLeases() {
    assert primaryMode;
    for (ShardRouting shardRouting : routingTable.assignedShards()) {
        RetentionLease lease = retentionLeases.get(getPeerRecoveryRetentionLeaseId(shardRouting));
        CheckpointState state = checkpoints.get(shardRouting.allocationId().getId());
        long newRetainedSeqNo = Math.max(0L, state.globalCheckpoint + 1L);
        if (lease.retainingSequenceNumber() <= newRetainedSeqNo) {
            renewRetentionLease(getPeerRecoveryRetentionLeaseId(shardRouting), newRetainedSeqNo, ...);
        }
    }
}
```

Primary 知道每个 replica 的 global checkpoint（因为 replica 会汇报），所以它直接把 peer recovery 租约推进到对应 replica 的 `global checkpoint + 1`。意思是："这个 replica 已经确认到这了，更早的操作我不需要再为它保留了"。

### 12.5 Peer Recovery 与 CCR 租约的区别

| 特征     | Peer Recovery 租约                         | CCR 租约                              |
|----------|--------------------------------------------|---------------------------------------|
| 谁创建   | Primary 自动为每个 replica 创建            | Follower 通过远程请求在 Leader 上创建 |
| 谁续期   | Primary 自己推进                           | Follower 每 30 秒远程续期             |
| 过期逻辑 | 特殊规则（节点不存在时才过期）             | 通用规则（12 小时未续期过期）         |
| 目的     | 让 replica 恢复时可以走 ops-based recovery | 让 follower 能持续读取历史操作        |
| source   | "peer recovery"                            | "ccr"                                 |

### 12.6 一张图总结

```
Primary Shard (Node-A) 的 ReplicationTracker:

┌────────────────────────────────────────────────────────────────┐
│ RetentionLeases (version=15, primaryTerm=3)                    │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ peer_recovery/node-A    seqNo=9500  (primary 自己)      │   │
│  │ peer_recovery/node-B    seqNo=9200  (replica-1)         │   │
│  │ peer_recovery/node-C    seqNo=9000  (replica-2)         │   │
│  │ dc2/follower-following-dc1/leader   seqNo=7000  (CCR-1) │   │
│  │ dc3/follower-following-dc1/leader   seqNo=3000  (CCR-2) │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                │
│  → SoftDeletesPolicy: min(9500,9200,9000,7000,3000) = 3000    │
│  → 保留 seq_no >= 3000 的 soft-deleted 文档                    │
│                                                                │
│  onSyncRetentionLeases → 同步到 Node-B, Node-C 的 replica     │
└────────────────────────────────────────────────────────────────┘
```

---

## 十三、代码执行位置：谁在哪执行什么

### 13.1 角色定义

```
单集群内（主副本复制）:
  Primary shard（主分片）  ←→  Replica shard（副本分片）

跨集群（CCR）:
  Leader shard（Leader 集群的 primary）  ←→  Follower shard（Follower 集群的 primary）
```

注意：Follower shard 在自己的集群里是 primary（它有自己的 replica）。所以一个 shard 可能同时是：
- 自己集群的 primary
- CCR 关系中的 follower

### 13.2 ReplicationTracker 中的函数——谁在执行

`ReplicationTracker` 存在于每个 shard（无论 primary 还是 replica）上，但大部分写操作只有 primary 才能执行：

```java
// ===== Primary 执行的（assert primaryMode） =====

addRetentionLease(...)           // primary 上添加租约
renewRetentionLease(...)         // primary 上续期租约
removeRetentionLease(...)        // primary 上删除租约
getRetentionLeases(expireLeases=true)  // primary 上计算过期
renewPeerRecoveryRetentionLeases()     // primary 为每个 replica 推进 peer recovery 租约

// ===== Replica 执行的（assert primaryMode == false） =====

updateRetentionLeasesOnReplica(...)    // replica 被动接收 primary 同步来的租约集合

// ===== 两者都执行的 =====

getRetentionLeases(expireLeases=false) // 读取当前租约集合，谁都能读
loadRetentionLeases(path)              // 节点启动时从磁盘加载
```

### 13.3 CcrRetentionLeases 中的函数——谁在执行

这些函数都是 **Follower 节点执行的**，但作用目标是 Leader 上的 primary shard：

```java
// ===== 全部在 Follower 节点执行，通过 RemoteClusterClient 发到 Leader =====

asyncAddRetentionLease(leaderShardId, ..., remoteClient, ...)
  → Follower 发请求给 Leader: "在你的 shard 上帮我创建一个租约"

asyncRenewRetentionLease(leaderShardId, ..., remoteClient, ...)
  → Follower 发请求给 Leader: "帮我续期"

asyncRemoveRetentionLease(leaderShardId, ..., remoteClient, ...)
  → Follower 发请求给 Leader: "帮我删掉租约"
```

执行路径：

```
Follower 节点                          Leader 节点
     │                                      │
     │  remoteClient.execute(REMOTE_ADD)    │
     ├─────────────────────────────────────→│
     │                                      │  Leader 的 primary shard 执行:
     │                                      │  ReplicationTracker.addRetentionLease()
     │                                      │  → 同步到 Leader 的 replica
     │          response                    │
     │←─────────────────────────────────────┤
```

### 13.4 SoftDeletesPolicy——谁在执行

`SoftDeletesPolicy` 存在于**每个 shard**（primary 和 replica 都有），因为每个 shard 都会独立 merge：

```java
// 每个 shard（primary 和 replica）在 merge 时都会调用
getMinRetainedSeqNo()    // 计算保留下限
getRetentionQuery()      // 生成 merge 时的保留 query
```

这就是为什么 primary 要同步租约到 replica——replica 在自己的 merge 中也需要知道保留下限。

### 13.5 总结对照表

| 函数/类                                             | 执行位置      | 身份               | 作用目标                           |
|-----------------------------------------------------|---------------|--------------------|------------------------------------|
| ReplicationTracker.addRetentionLease                | 本节点        | Primary            | 修改本 shard 的租约                |
| ReplicationTracker.renewRetentionLease              | 本节点        | Primary            | 修改本 shard 的租约                |
| ReplicationTracker.updateRetentionLeasesOnReplica   | 本节点        | Replica            | 接收 primary 同步来的租约          |
| ReplicationTracker.renewPeerRecoveryRetentionLeases | 本节点        | Primary            | 推进本 shard 的 peer recovery 租约 |
| CcrRetentionLeases.asyncAddRetentionLease           | Follower 节点 | Follower           | 远程在 Leader 的 shard 上创建租约  |
| CcrRetentionLeases.asyncRenewRetentionLease         | Follower 节点 | Follower           | 远程在 Leader 的 shard 上续期      |
| SoftDeletesPolicy.getMinRetainedSeqNo              | 本节点        | Primary 或 Replica | 控制本 shard 的 merge 保留         |
| LuceneChangesSnapshot                               | Leader 节点   | Leader Primary     | 从本地 Lucene 读历史操作           |

### 13.6 判断技巧

看代码时快速判断"谁在执行"：

1. 看到 `assert primaryMode` → 这个 shard 当前是 primary
2. 看到 `assert primaryMode == false` → 这个 shard 当前是 replica
3. 看到 `remoteClient.execute(...)` → 当前节点是 follower，请求发到 leader
4. 看到 `TransportAction` 的 `shardOperation()` → 请求到达目标节点后在那里执行
5. 看到 `synchronized(this)` + 操作 `retentionLeases` 或 `checkpoints` → 在 ReplicationTracker 所属的那个 shard 上执行

---

## 涉及的源码文件

- `server/src/main/java/org/elasticsearch/index/seqno/RetentionLease.java` — 租约数据结构定义
- `server/src/main/java/org/elasticsearch/index/seqno/RetentionLeases.java` — 租约集合，版本化管理
- `server/src/main/java/org/elasticsearch/index/seqno/ReplicationTracker.java` — 租约的 add/renew/remove/expire 操作，peer recovery 租约管理
- `server/src/main/java/org/elasticsearch/index/IndexSettings.java` — `index.soft_deletes.retention_lease.period` 配置
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/CcrRetentionLeases.java` — CCR 对租约的远程操作封装
- `server/src/main/java/org/elasticsearch/index/engine/SoftDeletesPolicy.java` — 取所有租约最小值计算保留下限
