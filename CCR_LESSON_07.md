# 第 6 课：CCR 是什么、解决什么问题

## 一、什么是 CCR

CCR = Cross-Cluster Replication（跨集群复制）。

一句话：**把一个集群（Leader）中索引的数据，实时复制到另一个集群（Follower）的索引中。**

```
Leader 集群（DC-1）                    Follower 集群（DC-2）
┌─────────────────────┐               ┌─────────────────────┐
│  orders 索引         │               │  orders 索引         │
│  [shard-0] [shard-1] │   ──复制──→   │  [shard-0] [shard-1] │
│                     │               │                     │
│  可读可写            │               │  只读                │
└─────────────────────┘               └─────────────────────┘
```

关键特征：
- **异步复制**：不影响 leader 写入延迟
- **拉取模式**：follower 主动从 leader 拉数据（pull-based）
- **单向**：leader 不知道有哪些 follower
- **只读**：follower 索引不允许直接写入

## 二、解决什么问题

| 场景 | 说明 |
|------|------|
| 灾备 | 主数据中心挂了，备份数据中心能立刻接管读取 |
| 就近读取 | 各地用户读本地 follower，减少延迟，所有写入仍走 leader |
| 集中报表 | 多个业务集群数据汇聚到报表集群做统一查询 |

## 三、CCR vs 主副本复制

| 维度 | 主副本复制 | CCR |
|------|-----------|-----|
| 范围 | 同一集群内 | 跨集群 |
| 方向 | Primary → Replica（push） | Follower ← Leader（pull） |
| 同步性 | 同步 | 异步 |
| 写入 | Primary 负责 | Leader 写入，Follower 只读 |
| 引擎 | InternalEngine | Follower 用 FollowingEngine |
| 数据来源 | Translog 操作直接转发 | 从 Lucene soft-deleted 文档中读取 |

## 四、Leader / Follower 模型

- **Leader 索引**：普通索引，可读写，不需要特殊配置，甚至不知道被 follow（除了 retention lease）
- **Follower 索引**：只读索引，使用 `FollowingEngine`，通过 persistent task 持续拉取数据。在自己集群内是 primary shard（有自己的 replica）

## 五、两阶段工作流程

### 阶段一：初始引导（Bootstrap）

通过 Snapshot/Restore 机制从 Leader 拷贝全量数据（使用 `CcrRepository`），恢复完成后记录断点（global checkpoint）。

### 阶段二：持续复制（Ongoing Replication）

从断点开始，持续增量拉取。每个 shard 一个 persistent task 驱动复制循环。

## 六、拉取机制——长轮询（Long Polling）

CCR **不是定时轮询**，而是长轮询，及时性接近实时推送。

### 工作方式

```
Follower: "给我 seq_no > 5000 的操作"
Leader:
  有新数据 → 立刻返回
  没有新数据 → 注册 GlobalCheckpointListener，挂起等待
                → 新数据写入，globalCheckpoint 推进
                → listener 触发，立刻返回数据
                → 超时（默认 1 分钟）未有新数据 → 返回空响应
```

### Leader 端核心代码（`ShardChangesAction`）

```java
if (request.getFromSeqNo() > seqNoStats.getGlobalCheckpoint()) {
    // 没有新数据，注册 listener 等待 globalCheckpoint 推进
    indexShard.addGlobalCheckpointListener(
        request.getFromSeqNo(),
        (globalCheckpoint, exception) -> {
            // 有新数据了，立刻返回
            globalCheckpointAdvanced(shardId, globalCheckpoint, request, listener);
        },
        request.getPollTimeout()  // 默认 1 分钟
    );
} else {
    // 有新数据，立刻读取返回
    super.asyncShardOperation(request, shardId, listener);
}
```

### 实际延迟

- **有持续写入时**：几乎实时。新数据写入 → listener 立刻触发 → follower 拿到数据。延迟 = 网络往返 + follower 写入时间（毫秒级）
- **没有写入时**：请求挂起等待，不消耗 CPU/IO

### 为什么不用 Push

- Pull 架构下 leader 不需要维护 follower 列表，不需要处理推送失败
- Follower 自己控制拉取速度（背压控制）
- Follower 挂了 leader 无感知，恢复后自行追赶
- 与 Remote Cluster 单向连接模型匹配

## 七、前置知识串联

```
第 1 课 seq_no & checkpoint  → Follower 用 globalCheckpoint 追踪复制进度
第 2 课 Soft Deletes         → Leader 保留历史操作供 Follower 读取
第 3 课 Retention Lease      → Follower 防止 Leader 清除它需要的历史操作
第 4 课 PersistentTasks      → 每个 shard 的复制循环是一个 persistent task
第 5 课 Remote Cluster       → Follower 通过 RemoteClusterClient 向 Leader 发请求
```

## 八、REST API 操作一览

```bash
# 配置远程集群
PUT _cluster/settings
{ "persistent": { "cluster.remote.leader_cluster.seeds": ["10.0.0.1:9300"] } }

# 开始 follow
PUT /orders-follower/_ccr/follow
{ "remote_cluster": "leader_cluster", "leader_index": "orders" }

# 查看状态
GET /orders-follower/_ccr/stats

# 暂停 / 恢复 / 彻底停止
POST /orders-follower/_ccr/pause_follow
POST /orders-follower/_ccr/resume_follow
POST /orders-follower/_ccr/unfollow

# 自动 follow 新索引
PUT /_ccr/auto_follow/my_pattern
{ "remote_cluster": "leader_cluster", "leader_index_patterns": ["logs-*"] }
```

## 九、小结

| 概念 | 含义 |
|------|------|
| CCR | 跨集群异步复制，follower 从 leader 拉取数据 |
| Leader | 普通可读写索引，被动提供数据 |
| Follower | 只读索引，通过 persistent task 持续拉取 |
| 长轮询 | 有数据立刻返回，没数据挂起等待（默认最多 1 分钟） |
| 两阶段 | Bootstrap（全量快照）+ Ongoing（增量长轮询） |
| 使用场景 | 灾备、就近读取、集中报表 |

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardChangesAction.java` — Leader 端长轮询实现，`GlobalCheckpointListener` 等待机制
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/TransportResumeFollowAction.java` — `DEFAULT_READ_POLL_TIMEOUT`（1 分钟）
