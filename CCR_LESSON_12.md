# 第 12 课：ShardFollowTasksExecutor — 任务启动与元数据同步

## 一、核心问题

第 9 课最后一步 `initiateFollowing()` 调用 ResumeFollowAction，为每个分片创建了一个 PersistentTask。那么：

1. 这个任务分配到哪个节点执行？
2. 任务到达节点后，具体做什么？

`ShardFollowTasksExecutor` 就是回答这两个问题的类。

---

## 二、类的定位

```java
public final class ShardFollowTasksExecutor extends PersistentTasksExecutor<ShardFollowTask>
```

继承 `PersistentTasksExecutor`，是 CCR 复制任务的**执行器**。PersistentTasks 框架提供了模板方法：

| 模板方法 | 职责 |
|---------|------|
| `validate()` | 分配前校验 |
| `getAssignment()` | 选择执行节点 |
| `createTask()` | 在目标节点创建任务实例 |
| `nodeOperation()` | 启动业务逻辑 |

CCR 只需要重写这 4 个方法，框架负责其余一切（持久化、故障转移、重新分配等）。

---

## 三、执行顺序详解

### 阶段一：构造函数（节点启动时）

```java
public ShardFollowTasksExecutor(Client client, ThreadPool threadPool, ClusterService clusterService, SettingsModule settingsModule) {
    super(ShardFollowTask.NAME, threadPool.executor(Ccr.CCR_THREAD_POOL_NAME));
    this.client = client;
    this.threadPool = threadPool;
    this.ccrExecutor = getExecutor();
    this.clusterService = clusterService;
    this.indexScopedSettings = settingsModule.getIndexScopedSettings();
    this.retentionLeaseRenewInterval = CcrRetentionLeases.RETENTION_LEASE_RENEW_INTERVAL_SETTING.get(settingsModule.getSettings());
    this.waitForMetadataTimeOut = CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT.get(settingsModule.getSettings());
    clusterService.getClusterSettings()
        .addSettingsUpdateConsumer(CcrSettings.CCR_WAIT_FOR_METADATA_TIMEOUT, newVal -> this.waitForMetadataTimeOut = newVal);
}
```

- `super(ShardFollowTask.NAME, executor)` — 告诉框架："我负责处理名为 `ShardFollowTask.NAME` 的任务，用 CCR 专用线程池执行"
- 存下各种依赖：client、threadPool、clusterService、indexScopedSettings
- 读取两个配置：租约续期间隔、等待元数据超时
- 注册动态配置监听：`waitForMetadataTimeOut` 可以运行时修改

---

### 阶段二：validate() — 分配前校验

```java
@Override
public void validate(ShardFollowTask params, ClusterState clusterState) {
    final IndexRoutingTable routingTable = clusterState.getRoutingTable().index(params.getFollowShardId().getIndex());
    final ShardRouting primaryShard = routingTable.shard(params.getFollowShardId().id()).primaryShard();
    if (primaryShard.active() == false) {
        throw new IllegalArgumentException("The primary shard of a follower index " + primaryShard + " is not active");
    }
}
```

逻辑简单：**Follower 分片的主分片必须是 active 状态**。如果分片还没恢复好（比如 bootstrap 还没完成），任务不会被分配，等分片就绪后框架再次尝试。

---

### 阶段三：getAssignment() — 选择执行节点

```java
@Override
public Assignment getAssignment(ShardFollowTask params, Collection<DiscoveryNode> candidateNodes, ClusterState clusterState) {
    final DiscoveryNode node = selectLeastLoadedNode(
        clusterState,
        candidateNodes,
        ((Predicate<DiscoveryNode>) DiscoveryNode::canContainData).and(DiscoveryNode::isRemoteClusterClient)
    );
    if (node == null) {
        return NO_ASSIGNMENT;
    } else {
        return new Assignment(node.getId(), "node is the least loaded data node and remote cluster client");
    }
}
```

选节点的条件是**双重过滤**：
1. 必须是 data 节点（`canContainData`）— 因为要读写本地分片数据
2. 必须是 remote_cluster_client（`isRemoteClusterClient`）— 因为要连 Leader 集群拉数据

在满足条件的节点中，选**负载最轻**的那个（`selectLeastLoadedNode` 是框架方法）。

如果找不到符合条件的节点，返回 `NO_ASSIGNMENT`（"no nodes found with data and remote cluster client roles"），任务暂不分配，等有合适节点时再分。

---

### 阶段四：createTask() — 创建任务实例

```java
@Override
protected AllocatedPersistentTask createTask(
    long id, String type, String action, TaskId parentTaskId,
    PersistentTasksCustomMetadata.PersistentTask<ShardFollowTask> taskInProgress, Map<String, String> headers) {
    
    ShardFollowTask params = taskInProgress.getParams();
    Client followerClient = wrapClient(client, params.getHeaders(), clusterService.state());
    BiConsumer<TimeValue, Runnable> scheduler = (delay, command) -> 
        threadPool.scheduleUnlessShuttingDown(delay, ccrExecutor, command);
    final String recordedLeaderShardHistoryUUID = getLeaderShardHistoryUUID(params);
    
    return new ShardFollowNodeTask(id, type, action, ..., params, scheduler, System::nanoTime) {
        @Override protected void innerUpdateMapping(...) { ... }
        @Override protected void innerUpdateSettings(...) { ... }
        @Override protected void innerUpdateAliases(...) { ... }
        @Override protected void innerSendBulkShardOperationsRequest(...) { ... }
        @Override protected void innerSendShardChangesRequest(...) { ... }
        @Override protected Scheduler.Cancellable scheduleBackgroundRetentionLeaseRenewal(...) { ... }
    };
}
```

#### 逐步做了什么

1. **取出 params** — 从持久化任务中获取 ShardFollowTask 参数（remoteCluster、leaderShardId、followerShardId、各种配额）

2. **包装 followerClient** — 带上原始用户身份，后续对 Follower 的操作以发起 PUT follow 的用户权限执行

3. **创建 scheduler** — 给 ShardFollowNodeTask 提供"延迟执行"能力（重试时 delay 一段时间），NodeTask 自己不持有 ThreadPool

4. **获取 leader history UUID** — 从 Follower 索引的自定义元数据中取出 Leader 分片的 historyUUID，后续读 Leader 时做一致性校验

5. **创建匿名子类** — `ShardFollowNodeTask` 是抽象类，定义了复制循环的状态机逻辑，但具体的 RPC 调用留给子类实现

#### 为什么用匿名内部类

`ShardFollowNodeTask` 像一个**调度员**（状态机），它决定"什么时候读、读多少、什么时候写、怎么重试"。但具体"怎么发网络请求"它不管。

这样设计的好处：
- **生产环境**：这里的匿名类提供真正的 RPC 实现（通过 remoteClient 连 Leader）
- **单元测试**：测试代码用另一个子类，直接返回 mock 数据，不需要启动真实集群

匿名内部类天然能访问外层 Executor 的成员变量（client、clusterService、remoteClient() 等），写起来最简洁。

---

### 阶段五：nodeOperation() — 启动业务逻辑

这是任务被分配到节点后，框架调用的**启动入口**。

#### 逻辑

做的事情只有一件：**查出 Follower 分片当前复制到了哪里（globalCheckpoint），然后从那个位置开始启动复制循环。**

为什么需要查？
- 第一次启动（刚 PUT follow）：bootstrap 完成后 Follower 已有初始数据，checkpoint 不是 0
- 恢复启动（节点重启/任务重新分配）：Follower 之前复制了一部分，checkpoint 记录了上次位置

不管哪种情况，都需要先知道"从哪开始"。

#### 逐行讲解

```java
@Override
protected void nodeOperation(final AllocatedPersistentTask task, final ShardFollowTask params, final PersistentTaskState state) {
    Client followerClient = wrapClient(client, params.getHeaders(), clusterService.state());
    ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
    logger.info("{} Starting to track leader shard {}", params.getFollowShardId(), params.getLeaderShardId());
```

- 包装带用户身份的 Client
- 向下转型为 ShardFollowNodeTask（框架传入的是 AllocatedPersistentTask 类型）
- 打一条日志记录任务启动

```java
    FollowerStatsInfoHandler handler = (followerHistoryUUID, followerGCP, maxSeqNo) -> {
        shardFollowNodeTask.start(followerHistoryUUID, followerGCP, maxSeqNo, followerGCP, maxSeqNo);
    };
```

定义**成功回调**。查到 Follower 分片状态后，调用 `start()` 启动复制循环。5 个参数含义：
- `followerHistoryUUID` — Follower 分片的历史标识
- `followerGCP` — 当前 global checkpoint（"已确认复制到这里"）
- `maxSeqNo` — 当前最大序列号
- 第 4、5 个参数是状态机内部的初始值（lastFetchedSeqNo、followerMaxSeqNo）

本质上就是告诉状态机：**你的起点在这里，从这个位置开始向 Leader 拉取。**

```java
    Consumer<Exception> errorHandler = e -> {
        if (shardFollowNodeTask.isStopped()) {
            return;
        }

        if (ShardFollowNodeTask.shouldRetry(e)) {
            logger.debug(() -> format("failed to fetch follow shard global %s checkpoint and max sequence number",
                shardFollowNodeTask), e);
            try {
                threadPool.schedule(() -> nodeOperation(task, params, state), params.getMaxRetryDelay(), ccrExecutor);
            } catch (EsRejectedExecutionException rex) {
                rex.addSuppressed(e);
                shardFollowNodeTask.onFatalFailure(rex);
            }
        } else {
            shardFollowNodeTask.onFatalFailure(e);
        }
    };
```

定义**失败回调**：
- 任务已停止 → 忽略
- 可重试错误（网络超时、分片暂时不可用等）→ 延迟 `maxRetryDelay` 后**递归调用 nodeOperation()**
- 如果连调度都被拒绝（线程池满/节点关闭）→ 致命错误
- 不可重试错误（权限不足、索引不存在等）→ 直接标记任务致命失败

```java
    fetchFollowerShardInfo(followerClient, params.getFollowShardId(), handler, errorHandler);
}
```

最后一行发起查询，把成功和失败回调传入。

#### fetchFollowerShardInfo — 查询 Follower 分片状态

```java
private void fetchFollowerShardInfo(Client followerClient, ShardId shardId,
    FollowerStatsInfoHandler handler, Consumer<Exception> errorHandler) {
    
    followerClient.admin().indices().stats(new IndicesStatsRequest().indices(shardId.getIndexName()), ActionListener.wrap(r -> {
        // 1. 从响应中找到对应索引的统计
        IndexStats indexStats = r.getIndex(shardId.getIndexName());
        if (indexStats == null) { ... errorHandler ... return; }
        
        // 2. 从所有分片统计中过滤出我们要的主分片
        Optional<ShardStats> filteredShardStats = Arrays.stream(indexStats.getShards())
            .filter(shardStats -> shardStats.getShardRouting().shardId().equals(shardId))
            .filter(shardStats -> shardStats.getShardRouting().primary())
            .findAny();
        
        // 3. 从分片统计中提取三个关键值
        if (filteredShardStats.isPresent()) {
            final CommitStats commitStats = shardStats.getCommitStats();  // 可能为null（分片正在关闭）
            final SeqNoStats seqNoStats = shardStats.getSeqNoStats();    // 可能为null
            
            final String historyUUID = commitStats.getUserData().get(Engine.HISTORY_UUID_KEY);
            final long globalCheckpoint = seqNoStats.getGlobalCheckpoint();
            final long maxSeqNo = seqNoStats.getMaxSeqNo();
            handler.accept(historyUUID, globalCheckpoint, maxSeqNo);  // → 触发 start()
        }
    }, errorHandler));
}
```

纯本地操作（Follower 集群内部），通过标准的 IndicesStatsRequest 查询分片的：
- `historyUUID` — 分片的历史标识，防止混淆不同生命周期的同名分片
- `globalCheckpoint` — "已确认复制到这里"
- `maxSeqNo` — "当前收到的最大序列号"

---

## 四、匿名类实现的 6 个方法（逻辑概述）

这 6 个方法不是在 `createTask()` 时执行的。它们是给 ShardFollowNodeTask 状态机**装上的具体能力**，由状态机在运行过程中按需调用。

### 为什么需要这 6 个方法

CCR 的持续复制不只是"读操作 + 写操作"。想象如果只做读写：
- Leader 加了新字段 → Follower 的 mapping 没更新 → 写入失败
- Leader 改了 settings → Follower 不知道 → 行为不一致
- Leader 的历史数据被 merge 清理 → Follower 还没拉完 → 数据丢失

所以 6 件事缺一不可：

### 1. innerSendShardChangesRequest — 从 Leader 读操作

向 Leader 分片发 RPC："从 seqNo=X 开始，给我最多 N 条写操作"。Leader 从 Lucene 中按 seqNo 读出操作返回。如果暂时没新数据，Leader 会长轮询等待。

**方向**：Follower → Leader（RPC）
**触发时机**：每轮复制循环

### 2. innerSendBulkShardOperationsRequest — 向 Follower 写操作

把从 Leader 读回来的操作列表批量写入 Follower 本地分片。写入时使用 Leader 的 seqNo（保证两边序列号一致）。

**方向**：Follower 本地执行
**触发时机**：读回来之后

### 3. innerUpdateMapping — 同步 Mapping

两步：先向 Leader 发 RPC 请求最新 mapping（带版本号要求，Leader 长轮询等待），拿到后在 Follower 本地执行 putMapping。

**方向**：Leader 读 + Follower 写
**触发时机**：状态机发现 Leader 操作引用了更高版本的 mapping 时

### 4. innerUpdateSettings — 同步 Settings

从 Leader 拿最新 settings，和 Follower 对比后：
- 没差异 → 跳过
- 只有动态设置变了 → 直接热更新
- 有静态设置变了 → 必须"关索引 → 改设置 → 开索引"

**方向**：Leader 读 + Follower 写
**触发时机**：settings 版本落后时

### 5. innerUpdateAliases — 同步 Aliases

从 Leader 拿当前别名列表，和 Follower 三路对比：Leader 有的要加、Leader 没的要删、不一样的以 Leader 为准。write alias 强制设为 false。

**方向**：Leader 读 + Follower 写
**触发时机**：aliases 版本落后时

### 6. scheduleBackgroundRetentionLeaseRenewal — 续期保留租约

启动定时器，每隔固定间隔向 Leader 发续期 RPC，告诉它"我复制到了 globalCheckpoint+1，请保留之后的历史数据"。如果租约丢失会尝试重新添加。

**方向**：定时 RPC 到 Leader
**触发时机**：任务启动后持续运行

### 总结对照

| 方法 | 一句话 | 方向 | 触发时机 |
|------|--------|------|---------|
| innerSendShardChangesRequest | 从 Leader 拉新操作 | → Leader | 每轮复制循环 |
| innerSendBulkShardOperationsRequest | 把操作写入 Follower | Follower 本地 | 读回来之后 |
| innerUpdateMapping | 同步字段定义 | Leader读 + Follower写 | mapping 版本落后时 |
| innerUpdateSettings | 同步索引配置 | Leader读 + Follower写 | settings 版本落后时 |
| innerUpdateAliases | 同步别名 | Leader读 + Follower写 | aliases 版本落后时 |
| scheduleBackgroundRetentionLeaseRenewal | 保住历史数据不被清理 | → Leader（定时） | 任务启动后持续运行 |

前 5 个是"按需执行"（状态机判断需要时才调），第 6 个是"启动后一直跑"（定时器驱动）。

---

## 五、整体执行时间线

```
PersistentTasks 框架收到 ShardFollowTask
  │
  ├─ validate() → follower 主分片必须 active
  │
  ├─ getAssignment() → 选 data + remote_cluster_client 角色的最轻节点
  │
  ├─ 框架把任务分配到目标节点
  │
  ├─ createTask() → 创建 ShardFollowNodeTask 实例，注入 6 个 RPC 实现
  │
  └─ nodeOperation()
        │
        ├─ fetchFollowerShardInfo() → 查 Follower 分片的 globalCheckpoint
        │     │
        │     ├─ 成功 → shardFollowNodeTask.start() → 进入复制循环
        │     │
        │     └─ 失败
        │           ├─ 可重试 → delay 后递归调用 nodeOperation()
        │           └─ 不可重试 → onFatalFailure()
        │
        ▼ 复制循环运行中...
        │
        ├─ innerSendShardChangesRequest()       → RPC到Leader读操作
        ├─ innerSendBulkShardOperationsRequest() → 本地写入Follower
        ├─ innerUpdateMapping()                 → Leader读 + Follower写
        ├─ innerUpdateSettings()                → Leader读 + Follower写
        ├─ innerUpdateAliases()                 → Leader读 + Follower增删
        └─ scheduleBackgroundRetentionLeaseRenewal() → 定期RPC续期租约
```

---

## 六、哪些是 ES 框架已有的，哪些是 CCR 自己写的

### CCR 自己写的

| 组件 | 作用 |
|------|------|
| `ShardFollowTasksExecutor` | 复制任务的执行器：选节点、创建任务、启动逻辑 |
| `ShardFollowNodeTask`（抽象类） | 复制循环状态机（下一课详讲） |
| 6 个匿名类方法 | 给状态机提供具体的 RPC 实现 |
| `ShardFollowTask` | 任务参数定义（remoteCluster、shardId、各种配额） |

### ES 框架已有的

| 组件 | CCR 怎么用 |
|------|-----------|
| `PersistentTasksExecutor<T>` | 继承，框架提供 validate/getAssignment/createTask/nodeOperation 模板 |
| `selectLeastLoadedNode()` | 直接调用，框架根据条件选最轻节点 |
| `threadPool.schedule()` | 直接调用，延迟执行（重试时用） |
| `threadPool.scheduleWithFixedDelay()` | 直接调用，定期执行（租约续期） |
| `IndicesStatsRequest` | 直接用，查询分片统计信息 |
| `PutMappingRequest / UpdateSettingsRequest / IndicesAliasesRequest` | 直接用，更新索引元数据 |
| `ClusterStateAction.REMOTE_TYPE` | 直接调用，跨集群获取集群状态 |

### 注册位置（Ccr.java）

```java
// getPersistentTasksExecutor() 中返回 Executor
return List.of(new ShardFollowTasksExecutor(client, threadPool, clusterService, settingsModule));
```

---

## 七、核心设计思路

**分离关注点**：

- `ShardFollowTasksExecutor`：负责**基础设施** — 选节点、创建任务、注入 RPC 实现、启动
- `ShardFollowNodeTask`：负责**业务逻辑** — 什么时候读、读多少、什么时候写、怎么重试

Executor 是"配置层"，NodeTask 是"运行层"。Executor 告诉 NodeTask"去哪拿数据、往哪写"，NodeTask 自己决定"怎么调度、怎么重试"。

如果你要自研类似功能：
1. 继承 `PersistentTasksExecutor`，重写 4 个模板方法
2. 定义自己的 NodeTask 抽象类（状态机逻辑）
3. 在 `createTask()` 中用匿名类注入具体实现

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowTasksExecutor.java` — 本课主角
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowNodeTask.java` — 复制循环状态机（第 13-15 课）
- `x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/ccr/action/ShardFollowTask.java` — 任务参数定义
- `server/src/main/java/org/elasticsearch/persistent/PersistentTasksExecutor.java` — ES 框架基类
