# 第 5 课：Transport Action 框架与继承体系

## 一、核心问题

ES 中所有操作（用户 HTTP 请求、节点间 RPC）都是 Action。但不同操作的执行位置不同——有的必须在 Master 上，有的要在某个 Shard 所在节点上，有的要广播到所有节点。

这节课回答：**ES 提供了哪些 Transport 基类？它们各自帮你处理了什么？你只需要填什么业务逻辑？**

## 二、全局执行路径

无论是哪种 Action，统一走这条路径：

```
请求进来
  → TransportAction.execute()
      → 校验请求 (request.validate())
      → 执行 ActionFilter 链（Security 权限检查、审计）
      → doExecute()（子类实现：路由到正确节点）
          → 在目标节点执行业务逻辑
      → listener.onResponse() / onFailure()
  → 响应回去
```

## 三、完整继承链

```
TransportAction                          ← 校验 + Filter 链
    └── HandledTransportAction           ← 注册 Transport 处理器（让远端能调用）
            ├── TransportMasterNodeAction       ← 自动路由到 Master
            ├── TransportSingleShardAction      ← 自动路由到目标 Shard 所在节点
            ├── TransportBroadcastAction         ← 广播到所有相关 Shard
            └── TransportReplicationAction      ← Primary 写入 + Replica 同步
```

## 四、每层职责

### 第一层：`TransportAction`

| 自动做的事 | 说明 |
|-----------|------|
| 请求校验 | 调用 `request.validate()`，不合法直接 `listener.onFailure()` |
| Filter 链 | Security 插件的权限检查、审计日志在这里拦截 |
| 线程切换 | 如果配了非直接执行器，自动 fork 到指定线程池 |

定义的抽象方法：

```java
protected abstract void doExecute(Task task, Request request, ActionListener<Response> listener);
```

### 第二层：`HandledTransportAction`

| 自动做的事 | 说明 |
|-----------|------|
| 注册传输处理器 | 构造时调用 `transportService.registerRequestHandler(actionName, ...)` |

意义：当节点 A 把请求转发给节点 B 时，B 必须有注册对应 actionName 的 handler。这一层自动注册，使得其他节点能调用这个 Action。

### 第三层：具体路由策略（你选一个继承）

根据业务需求选择对应的基类。

## 五、常用基类详解

### 5.1 `TransportMasterNodeAction` — 在 Master 节点执行

**适用场景**：修改集群状态的操作（创建/删除索引、注册任务、修改 settings）

**自动做的事：**
- 判断当前节点是否 Master
- 不是 Master → 自动转发给 Master
- 没有 Master（选举中）→ 等待选举完成后重试
- 集群有 Block → 等待 Block 解除后重试
- 执行失败（发布 ClusterState 失败）→ 自动重试
- 超过 `masterNodeTimeout` → 自动失败

**你只需实现：**

```java
// 业务逻辑（保证运行在 Master 上）
protected abstract void masterOperation(Task, Request, ClusterState, ActionListener<Response>);

// Block 检查（返回 null 表示不阻塞）
protected abstract ClusterBlockException checkBlock(Request, ClusterState);
```

**ES 核心中的例子：**
- `TransportCreateIndexAction` — 用户调用 `PUT /my_index` 创建索引
- `TransportDeleteIndexAction` — 用户调用 `DELETE /my_index` 删除索引
- `TransportClusterUpdateSettingsAction` — 用户调用 `PUT _cluster/settings`

**CCR 中的例子：**
- `TransportPutFollowAction` — 创建 follower 索引
- `TransportPauseFollowAction` — 暂停 follow（删除 persistent task）

---

### 5.2 `TransportSingleShardAction` — 在某个 Shard 所在节点执行

**适用场景**：读取某个特定 shard 的数据

**自动做的事：**
- 根据请求中的 shard id / routing，定位目标 shard 在哪个节点
- 自动转发到那个节点执行
- 如果 Primary 不可用，尝试 Replica 所在的节点
- 处理 shard 不存在、正在迁移等情况

**你只需实现：**

```java
// 在目标 shard 所在节点执行的业务逻辑
protected abstract Response shardOperation(Request request, ShardId shardId) throws IOException;

// 解析请求，确定要去哪个 shard
protected abstract ShardsIterator shards(ClusterState state, InternalRequest request);
```

**ES 核心中的例子：**
- `TransportGetAction` — 用户调用 `GET /my_index/_doc/123`，按文档 ID 的路由找到目标 shard，读取并返回
- `TransportTermVectorsAction` — 获取文档的词向量信息

**CCR 中的例子：**
- `ShardChangesAction.TransportAction` — Follower 向 Leader 的某个 shard 请求增量操作

执行流程：
```
Follower: "给我 leader shard-0 从 seq_no=5000 开始的操作"
    → 框架找到 shard-0 在 Leader 集群的哪个节点
    → 转发到那个节点
    → shardOperation(): 读取 LuceneChangesSnapshot 返回数据
```

---

### 5.3 `TransportReplicationAction` — Primary 写入 + Replica 同步

**适用场景**：写入操作（需要先写 Primary，再同步到所有 Replica）

**自动做的事：**
- 路由到 Primary shard 所在节点
- 在 Primary 上执行写入
- 写入成功后，自动并行转发到所有 Replica shard
- 等待指定数量的 Replica 确认（wait_for_active_shards）
- 处理 Primary 迁移、Replica 失败、Replica 落后等复杂情况

**你只需实现：**

```java
// 在 Primary shard 上执行
protected abstract void shardOperationOnPrimary(
    Request request, IndexShard primary, ActionListener<PrimaryResult> listener);

// 在 Replica shard 上执行
protected abstract void shardOperationOnReplica(
    ReplicaRequest request, IndexShard replica, ActionListener<ReplicaResult> listener);
```

**ES 核心中的例子：**
- `TransportShardBulkAction` — 用户调用 `POST _bulk` 批量写入。所有文档写入的最终执行者。流程：
  ```
  用户: POST _bulk (100 条文档)
      → 按 routing 拆分到不同 shard
      → 每个 shard 的文档 → TransportShardBulkAction
          → shardOperationOnPrimary(): 在 Primary 写入 Lucene
          → 框架自动转发到 Replica
          → shardOperationOnReplica(): Replica 也写入
      → 全部完成 → 返回成功
  ```
- `TransportDeleteAction` — 删除单条文档（也是 Primary+Replica）

**CCR 中的例子：**
- `TransportBulkShardOperationsAction` — 把从 Leader 拉来的操作批量写入 Follower shard

---

### 5.4 `TransportBroadcastAction` — 广播到所有 Shard

**适用场景**：需要在所有（或多个）shard 上执行的操作，汇总结果

**自动做的事：**
- 找到索引的所有 shard
- 并行发请求到每个 shard 所在的节点
- 收集所有 shard 的响应
- 汇总成最终响应返回

**你只需实现：**

```java
// 在每个 shard 上执行
protected abstract ShardResponse shardOperation(Request request, ShardRouting shard);

// 把所有 shard 的响应合并成总响应
protected abstract Response newResponse(Request request, int totalShards,
    int successfulShards, int failedShards,
    List<ShardResponse> responses,
    List<DefaultShardOperationFailedException> failures);
```

**ES 核心中的例子：**
- `TransportRefreshAction` — 用户调用 `POST /my_index/_refresh`，对所有 shard 执行 refresh 使新写入可见
- `TransportForceMergeAction` — 强制合并所有 shard 的 Lucene segments
- `TransportIndicesStatsAction` — 收集所有 shard 的统计信息（文档数、磁盘占用等）

**CCR 中的例子：**
- `TransportFollowStatsAction` — 收集所有 follower shard 的复制统计

---

## 六、ES 正常写入和搜索流程中的 Transport 使用

### 6.1 写入一条文档的完整 Transport 链

```
用户: PUT /orders/_doc/123 { "product": "phone" }
     │
     ▼
RestIndexAction (REST Handler)
     → client.execute(TransportIndexAction)
         │
         ▼
TransportBulkAction (extends TransportAction)
     │  单条写入也走 bulk 路径
     │  按 routing 确定文档属于哪个 shard
     ▼
TransportShardBulkAction (extends TransportReplicationAction)
     │  路由到 shard-0 的 Primary 节点
     │
     ├── shardOperationOnPrimary():
     │       InternalEngine.index() → 写入 Lucene + Translog
     │
     └── 框架自动同步 →
         shardOperationOnReplica():
             InternalEngine.index() → Replica 也写入
```

### 6.2 搜索请求的完整 Transport 链

```
用户: GET /orders/_search { "query": { "match": { "product": "phone" } } }
     │
     ▼
RestSearchAction (REST Handler)
     → client.execute(TransportSearchAction)
         │
         ▼
TransportSearchAction (extends HandledTransportAction)
     │  协调节点角色：拆分为 query + fetch 两阶段
     │
     ├── Query 阶段：
     │   向每个 shard 发 QuerySearchRequest (内部 Transport RPC)
     │   每个 shard 返回 top-N 的 doc id + score
     │   协调节点归并排序，选出全局 top-N
     │
     └── Fetch 阶段：
         向包含 top-N 文档的 shard 发 FetchSearchRequest
         每个 shard 返回完整文档内容
         协调节点组装最终响应返回用户
```

### 6.3 Get 单条文档

```
用户: GET /orders/_doc/123
     │
     ▼
RestGetAction → TransportGetAction (extends TransportSingleShardAction)
     │  按 doc id 的 routing 找到 shard-0
     │  找到 shard-0 所在节点（Primary 或 Replica 都行）
     │  转发到那个节点
     ▼
shardOperation():
     IndexShard.get(id) → 从 Lucene 读取文档返回
```

## 七、选型决策表

| 你的需求 | 选哪个基类 | 你写什么 |
|---------|-----------|---------|
| 修改集群元数据 | `TransportMasterNodeAction` | `masterOperation()` + `checkBlock()` |
| 读某个 shard 的数据 | `TransportSingleShardAction` | `shardOperation()` |
| 写入数据（Primary+Replica） | `TransportReplicationAction` | `shardOperationOnPrimary()` + `shardOperationOnReplica()` |
| 对所有 shard 执行并汇总 | `TransportBroadcastAction` | `shardOperation()` + `newResponse()` |
| 不涉及路由，当前节点执行 | `HandledTransportAction` | `doExecute()` |
| 只读集群状态（可在非 Master 执行） | `TransportMasterNodeReadAction` | `masterOperation()` + `checkBlock()` |

## 八、CCR 中各 Action 的基类选择

| CCR Action | 继承的基类 | 原因 |
|-----------|-----------|------|
| `TransportPutFollowAction` | `TransportMasterNodeAction` | 创建索引（修改集群状态） |
| `TransportPauseFollowAction` | `TransportMasterNodeAction` | 删除 persistent task |
| `TransportResumeFollowAction` | `TransportMasterNodeAction` | 创建 persistent task |
| `TransportUnfollowAction` | `TransportMasterNodeAction` | 修改索引 settings |
| `ShardChangesAction.TransportAction` | `TransportSingleShardAction` | 读 leader 某个 shard 的变更 |
| `TransportBulkShardOperationsAction` | `TransportReplicationAction` | 写入 follower shard |
| `TransportFollowStatsAction` | `TransportBroadcastAction` | 收集所有 shard 的统计 |

## 九、小结

**继承不同的基类 = 选择不同的路由策略。** 框架帮你处理"请求发到哪个节点"的所有复杂性（节点发现、转发、重试、failover），你只需要实现"到了目标节点之后做什么"的业务逻辑。

| 层级 | 类 | 职责 |
|-----|---|------|
| 第一层 | `TransportAction` | 请求校验 + ActionFilter 链 |
| 第二层 | `HandledTransportAction` | 注册传输处理器（让其他节点能调用你） |
| 第三层 | 具体基类 | 路由策略（Master / SingleShard / Replication / Broadcast） |
| 第四层 | 你的类 | 纯业务逻辑 |

---

## 涉及的源码文件

- `server/src/main/java/org/elasticsearch/action/support/TransportAction.java` — 所有 Action 的祖宗类
- `server/src/main/java/org/elasticsearch/action/support/HandledTransportAction.java` — 注册传输层处理器
- `server/src/main/java/org/elasticsearch/action/support/master/TransportMasterNodeAction.java` — Master 路由 + 重试
- `server/src/main/java/org/elasticsearch/action/support/single/shard/TransportSingleShardAction.java` — 单 Shard 路由
- `server/src/main/java/org/elasticsearch/action/support/replication/TransportReplicationAction.java` — Primary+Replica 写入
- `server/src/main/java/org/elasticsearch/action/support/broadcast/TransportBroadcastAction.java` — 广播到所有 Shard
