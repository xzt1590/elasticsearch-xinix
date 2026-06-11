# 第 4 课：PersistentTasks 框架

## 一、核心问题

CCR 的复制是一个**长期运行的后台任务**——每个 follower shard 需要一个循环不断从 leader 拉取数据。

问题：
- 运行任务的节点宕机了怎么办？
- 集群重启后这些任务还在吗？
- 谁来决定任务跑在哪个节点上？

普通的 `ThreadPool.execute()` 做不到这些。ES 需要一个"跨节点重启存活的后台任务框架"，这就是 **PersistentTasks**。

## 二、框架定位

PersistentTasks 是 **ES 原生框架**，代码在 `server/src/main/java/org/elasticsearch/persistent/` 包下，和 CCR 无关。CCR 只是这个框架的"用户"之一。

其他使用者：
- ML（机器学习）的 job 任务
- Transform（数据转换）任务
- Watcher 后台服务
- Health Node 选举

你完全可以写一个插件继承 `PersistentTasksExecutor` 来使用这个框架，不需要 CCR。

**设计哲学**：把任务信息写入 ClusterState，让它和集群一样长命。ClusterState 会持久化到磁盘（Gateway），集群重启后恢复，任务信息跟着走，所以任务"永生"——除非显式删除。

## 三、整体架构

```
用户请求（PUT /_ccr/follow）
  │
  ▼
PersistentTasksService         ← 通信门面，任何节点上发请求给 Master
  │
  ▼
PersistentTasksClusterService  ← 仅 Master 运行，负责任务分配/重分配，修改 ClusterState
  │
  ▼
ClusterState 广播到所有节点     ← 包含 PersistentTasksCustomMetadata
  │
  ▼
PersistentTasksNodeService     ← 每个节点运行，监听 ClusterState，启动/停止本地任务
  │
  ▼
PersistentTasksExecutor        ← 抽象基类，子类实现具体业务逻辑（如 CCR）
  │
  ▼
AllocatedPersistentTask        ← 任务在本地 TaskManager 中的运行实体
```

只有 Master 节点能修改 ClusterState。任何节点想创建/删除/修改 persistent task，都是发请求给 Master，Master 修改后广播给所有节点。

## 四、存储结构

### 4.1 ClusterState 中的存储形态

```json
{
  "metadata": {
    "persistent_tasks": {
      "last_allocation_id": 7,
      "tasks": [
        {
          "id": "ccr-orders-follower-0",
          "task_name": "xpack/ccr/shard_follow_task",
          "allocation_id": 6,
          "params": {
            "remote_cluster": "dc1",
            "leader_shard": "orders[0]",
            "follower_shard": "orders-follower[0]",
            "max_read_request_operation_count": 5120
          },
          "state": null,
          "assignment": {
            "executor_node": "node-C-id",
            "explanation": "node is the least loaded data node and remote cluster client"
          }
        }
      ]
    }
  }
}
```

外层包装（id、task_name、allocation_id、assignment）是**框架固定的**。`params` 和 `state` 的结构是**你自己定义的**。

### 4.2 框架固定的字段

| 字段 | 含义 |
|------|------|
| id | 任务唯一 ID（创建时指定） |
| task_name | 任务类型名（对应 executor 注册的名称） |
| allocation_id | 当前分配轮次（每次重分配 +1，防幽灵通知） |
| assignment.executor_node | 分配到哪个节点执行 |
| assignment.explanation | 分配原因说明 |

### 4.3 自定义的字段

**params**——实现 `PersistentTaskParams` 接口：

```java
public interface PersistentTaskParams extends NamedWriteable, ToXContentObject {
    String getWriteableName();  // 任务类型名
}
```

**state**——实现 `PersistentTaskState` 接口（用于断点续传）：

```java
public interface PersistentTaskState extends NamedWriteable, ToXContentObject {
}
```

框架只要求能序列化/反序列化（因为要写入 ClusterState 存磁盘），内部存什么完全由你决定。

### 4.4 对比：通用定义 vs CCR 定义

**通用任务参数（假设你自己写一个清理任务）**：

```java
public class CleanupTaskParams implements PersistentTaskParams {
    private final String indexName;
    private final long retentionDays;

    @Override
    public String getWriteableName() { return "my_cleanup_task"; }
}
```

**CCR 的任务参数 `ShardFollowTask`**：

```java
public class ShardFollowTask implements PersistentTaskParams {
    private final String remoteCluster;
    private final ShardId followShardId;
    private final ShardId leaderShardId;
    private final int maxReadRequestOperationCount;
    private final int maxWriteRequestOperationCount;
    private final ByteSizeValue maxReadRequestSize;
    // ... 还有很多配置

    @Override
    public String getWriteableName() { return "xpack/ccr/shard_follow_task"; }
}
```

## 五、按代码执行顺序讲解完整流程

### 第一步：注册 Executor（应用启动时）

在 Plugin 类中告诉 ES"我有一个任务类型"：

```java
// 通用写法
public class MyPlugin extends Plugin implements PersistentTaskPlugin {
    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(...) {
        return List.of(new CleanupTaskExecutor(threadPool));
    }
}
```

```java
// CCR 的写法（Ccr.java）
public class Ccr extends Plugin implements PersistentTaskPlugin {
    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(...) {
        return List.of(new ShardFollowTasksExecutor(...));
    }
}
```

注册后，`PersistentTasksExecutorRegistry` 把所有 executor 存入 Map：`taskName → executor`。

### 第二步：发起创建请求（业务代码的入口）

在任何节点上调用 `PersistentTasksService.sendStartRequest()`：

```java
// 通用调用
persistentTasksService.sendStartRequest(
    "my-task-1",              // 任务 ID（你自己取名）
    "my_cleanup_task",        // 任务类型名
    new CleanupTaskParams("logs", 30),  // 参数
    TimeValue.THIRTY_SECONDS,
    listener
);
```

```java
// CCR 的调用（在 TransportResumeFollowAction 中，为每个 shard 调一次）
persistentTasksService.sendStartRequest(
    "ccr-orders-follower-0",          // taskId
    ShardFollowTask.NAME,             // "xpack/ccr/shard_follow_task"
    new ShardFollowTask(remoteCluster, followShardId, leaderShardId, ...),
    TimeValue.THIRTY_SECONDS,
    listener
);
```

`sendStartRequest` 内部发了一个 `StartPersistentTaskAction` transport 请求到 Master 节点。

### 第三步：Master 处理请求，修改 ClusterState

Master 上的 `PersistentTasksClusterService.createPersistentTask()` 执行：

```java
// 提交一个 ClusterState 更新任务
submitUnbatchedTask("create persistent task", new ClusterStateUpdateTask() {
    @Override
    public ClusterState execute(ClusterState currentState) {

        // 1. 校验
        executor.validate(taskParams, currentState);

        // 2. 决定分配到哪个节点
        //    过滤掉 shutdown 中的节点，随机打乱候选列表
        //    调用 executor.getAssignment(params, candidateNodes, state)
        Assignment assignment = createAssignment(taskName, taskParams, currentState);

        // 3. 构建新的 PersistentTasksCustomMetadata
        builder.addTask(taskId, taskName, taskParams, assignment);
        // 内部: lastAllocationId++

        // 4. 返回新 ClusterState
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(...)
                .putCustom(PersistentTasksCustomMetadata.TYPE, builder.build()))
            .build();
    }
});
```

**只有 Master 能修改 ClusterState**，所有 ES 集群元数据变更都走 `ClusterStateUpdateTask` 这个路径。

### 第四步：ClusterState 广播，目标节点启动任务

新 ClusterState 广播到所有节点，每个节点的 `PersistentTasksNodeService.clusterChanged()` 触发：

```java
public void clusterChanged(ClusterChangedEvent event) {
    PersistentTasksCustomMetadata tasks = event.state().metadata().custom(PersistentTasksCustomMetadata.TYPE);

    for (PersistentTask<?> taskInProgress : tasks.tasks()) {
        // 这个任务分配给我了吗？
        if (taskInProgress.isAssigned() && isMyNode(taskInProgress)) {
            // 我本地已经在跑了吗？
            if (!runningTasks.containsKey(taskInProgress.getAllocationId())) {
                // 新任务，启动它
                startTask(taskInProgress);
            }
        }
    }
    // 反方向：本地在跑但 ClusterState 中没有的 → 取消
}
```

`startTask()` 内部：

```java
private void startTask(PersistentTask<?> taskInProgress) {
    // 1. 拿到 executor
    PersistentTasksExecutor<?> executor = registry.get(taskInProgress.getTaskName());

    // 2. 创建 AllocatedPersistentTask（调用 executor.createTask()）
    AllocatedPersistentTask task = taskManager.register("persistent", taskName, request);

    // 3. 初始化：注入 service、taskManager、taskId、allocationId
    task.init(persistentTasksService, taskManager, taskId, allocationId);

    // 4. 放入本地 map
    runningTasks.put(allocationId, task);

    // 5. 调用业务逻辑
    executor.nodeOperation(task, taskInProgress.getParams(), taskInProgress.getState());
    //                                 ↑ 你的参数               ↑ 上次保存的状态（首次为 null）
}
```

### 第五步：任务运行

**通用模式**——完成后结束：

```java
@Override
protected void nodeOperation(AllocatedPersistentTask task, CleanupTaskParams params, PersistentTaskState state) {
    // 做清理工作...
    deleteOldDocs(params.indexName, params.retentionDays);
    // 完了
    task.markAsCompleted();
}
```

**CCR 模式**——长期运行不结束：

```java
@Override
protected void nodeOperation(AllocatedPersistentTask task, ShardFollowTask params, PersistentTaskState state) {
    ShardFollowNodeTask shardFollowNodeTask = (ShardFollowNodeTask) task;
    // 获取 follower shard 当前的 globalCheckpoint
    // 启动无限循环的拉取/写入
    shardFollowNodeTask.start(params.getLeaderShardId(), followerGlobalCheckpoint, ...);
    // 这个方法不会 return，循环一直跑
    // 只有 pause/unfollow 时才 markAsCompleted
}
```

### 第六步：运行中保存进度（可选，断点续传）

```java
// 任务中途想保存进度
task.updatePersistentTaskState(new MyProgressState(processedCount));
// → 发请求到 Master → Master 更新 ClusterState 中该任务的 state 字段
```

重启后，新的 `nodeOperation` 调用会收到这个 state：

```java
nodeOperation(task, params, savedState);
//                           ↑ 上次保存的 MyProgressState
```

**CCR 没有使用这个机制**。CCR 重启后直接读 follower shard 的 `globalCheckpoint` 来确定从哪继续，因为 checkpoint 本身就持久化在 shard 的 Lucene commit 中了。

### 第七步：节点宕机 → 自动重分配

```
Node-B 宕机
  │
  ▼
Master 检测到节点离开，更新 ClusterState
  │
  ▼
PersistentTasksCustomMetadata.disassociateDeadNodes()
  → Node-B 上所有任务的 assignment.executorNode 设为 null
  │
  ▼
PersistentTasksClusterService.clusterChanged()
  → shouldReassignPersistentTasks() == true
  │
  ▼
reassignTasks()
  → 对每个 unassigned 的任务重新调用 executor.getAssignment()
  → 选出新节点，更新 ClusterState
  → allocationId + 1
  │
  ▼
新 ClusterState 广播 → 新节点的 PersistentTasksNodeService 启动任务
  → nodeOperation(task, params, lastSavedState)
```

触发重分配的条件不只是节点宕机：
- 节点加入/离开
- 路由表变化（shard 状态变了）
- 元数据变化
- Master 切换

### 第八步：任务结束

```java
// 正常完成
task.markAsCompleted();
// 或失败
task.markAsFailed(exception);
```

内部流程：
1. 本地状态 CAS：STARTED → COMPLETED
2. `taskManager.unregister(task)` 从本地 TaskManager 移除
3. `persistentTasksService.sendCompletionRequest()` 通知 Master
4. Master 从 ClusterState 中 `removeTask(id)` → 任务彻底消失

还有一种特殊情况——**本地中止**：

```java
task.markAsLocallyAborted("shard relocated");
```

这不会删除任务，而是通知 Master 重新分配。CCR 在 follower shard 迁移到其他节点时使用这个。

## 六、allocationId 的作用——防幽灵通知

场景：
1. Node-B 跑着任务 X（allocationId=5）
2. Node-B 网络闪断，Master 以为它宕了，把任务重分配给 Node-C（allocationId=6）
3. Node-B 恢复了，旧任务发了"我完成了"的通知到 Master

如果 Master 接受这个通知，就会错误删除正在 Node-C 上运行的新任务。

解决：Master 校验 allocationId：

```java
if (tasksInProgress.hasTask(id, allocationId)) {
    // 匹配，正常处理
    tasksInProgress.removeTask(id);
} else {
    // 不匹配（旧轮次的幽灵），忽略
}
```

## 七、CCR 接入框架的完整对比

| 步骤 | 框架要求 | CCR 的实现 |
|------|---------|-----------|
| 定义参数 | 实现 `PersistentTaskParams` | `ShardFollowTask`（包含远程集群名、leader/follower shard ID、各种限流参数） |
| 定义 Executor | 继承 `PersistentTasksExecutor` | `ShardFollowTasksExecutor` |
| 调度策略 | 覆盖 `getAssignment()` | 选有 data + remote_cluster_client 角色的最空闲节点 |
| 创建校验 | 覆盖 `validate()` | 检查 follower shard 的 primary 是否 active |
| 创建任务实体 | 覆盖 `createTask()` | 返回 `ShardFollowNodeTask`（注入 mapping/settings/aliases 同步回调） |
| 业务逻辑 | 实现 `nodeOperation()` | 获取 follower 的 globalCheckpoint，启动拉取循环 |
| 断点续传 | 用 `updatePersistentTaskState()` | 不用，直接读 follower shard 的 globalCheckpoint |
| 注册 | Plugin 实现 `PersistentTaskPlugin` | `Ccr.java` 中注册 |
| 触发创建 | 调用 `sendStartRequest()` | `TransportResumeFollowAction` 为每个 shard 调用一次 |

## 八、AllocatedPersistentTask 的状态机

```
STARTED（运行中）
  ├── markAsCompleted()       → COMPLETED     → 通知 Master 删除任务（生命周期结束）
  ├── markAsFailed(e)         → COMPLETED     → 通知 Master 删除任务（带失败原因）
  ├── markAsLocallyAborted()  → LOCAL_ABORTED → 通知 Master 重新分配（任务不删除）
  └── markAsCancelled()       → PENDING_CANCEL → 本地取消，不通知 Master（Master 已经知道了）
```

## 九、小结

| 概念 | 含义 |
|------|------|
| PersistentTasksCustomMetadata | ClusterState 中的任务"户口本"，持久化到磁盘 |
| PersistentTasksClusterService | Master 上的调度中心，负责分配和重分配 |
| PersistentTasksNodeService | 每个节点的执行官，监听 ClusterState 启停任务 |
| PersistentTasksExecutor | 抽象基类，子类实现调度策略和业务逻辑 |
| AllocatedPersistentTask | 任务运行实体，可更新状态、标记完成/失败/中止 |
| allocationId | 防幽灵通知的版本号，每次重分配 +1 |
| PersistentTaskState | 断点续传的状态，写入 ClusterState |
| params 和 state | 自定义结构，框架只要求可序列化 |

**核心理解**：PersistentTasks 本质上是一个"**由 ClusterState 驱动的分布式任务调度器**"。任务的所有关键信息（参数、分配节点、运行状态）都在 ClusterState 中，任何节点宕机或 Master 切换都不会丢失任务。CCR 为每个 follower shard 创建一个 persistent task，框架保证这些复制循环在集群中始终有人执行。

---

## 涉及的源码文件

- `server/src/main/java/org/elasticsearch/persistent/PersistentTasksExecutor.java` — 任务执行器抽象基类
- `server/src/main/java/org/elasticsearch/persistent/AllocatedPersistentTask.java` — 任务运行实体，状态机管理
- `server/src/main/java/org/elasticsearch/persistent/PersistentTasksService.java` — 通信门面，发请求给 Master
- `server/src/main/java/org/elasticsearch/persistent/PersistentTasksCustomMetadata.java` — ClusterState 中的任务元数据
- `server/src/main/java/org/elasticsearch/persistent/PersistentTasksNodeService.java` — 节点级服务，监听 ClusterState 启停任务
- `server/src/main/java/org/elasticsearch/persistent/PersistentTasksClusterService.java` — Master 上的调度中心，任务分配/重分配
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowTasksExecutor.java` — CCR 的 Executor 实现
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowTask.java` — CCR 的任务参数定义
