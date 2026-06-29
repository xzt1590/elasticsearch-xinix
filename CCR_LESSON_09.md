# 第 9 课：PUT follow 的完整流程

## 一、核心问题

用户敲下 `PUT /orders-follower/_ccr/follow` 之后，ES 内部到底发生了什么？

这是 CCR 的**起点**。在此之前 follower 索引不存在；在此之后，一个能持续追上 leader 的 follower 索引就建立起来了。

## 二、用户视角：请求与响应

### 请求

```
PUT /orders-follower/_ccr/follow
{
  "remote_cluster": "leader-cluster",
  "leader_index": "orders",
  "settings": {
    "index.number_of_replicas": 1
  }
}
```

- URL 里的 `orders-follower` → follower 索引名（从 URL 路径取，不在 body 里）
- `remote_cluster` → 远程集群名
- `leader_index` → 要复制的源索引
- `settings` → 可选，覆盖 follower 的部分本地设置

### 响应

```json
{
  "follow_index_created": true,
  "follow_index_shards_acked": true,
  "index_following_started": true
}
```

三个布尔值对应**三步走**的三个阶段结果。

## 三、整体调用链

```
用户: PUT /orders-follower/_ccr/follow {remote_cluster, leader_index}
   │
   ▼
RestPutFollowAction          [REST层] 解析 JSON → Request，转发给 Transport
   │  client.execute(PutFollowAction.INSTANCE, ...)
   ▼
TransportPutFollowAction     [Transport层, 在 master 节点执行]
   │
   ├─① masterOperation()
   │     • 检查本地许可证
   │     • 验证能连上 remote_cluster
   │     • 跨集群拉取 leader 索引元数据 ──────┐(异步回调)
   │                                          ▼
   ├─② createFollowerIndex()
   │     • 校验 leader: 存在? soft deletes? 非 searchable snapshot?
   │     • 构造"伪装的"RestoreSnapshotRequest
   │     • restoreService.restoreSnapshot() ──┐(异步)
   │                                          ▼
   ├─③ afterRestoreStarted()
   │     • 等 restore 完成 (创建索引+分片+传Lucene文件)
   │     • failedShards==0 ? 继续 : 提前返回 ──┐
   │                                          ▼
   └─④ initiateFollowing()
         • 调用 ResumeFollowAction
         • 创建 ShardFollowTask 持久化任务 → 进入持续复制循环
         • 返回 Response(created, acked, started)
```

## 四、第一层：REST 入口（RestPutFollowAction）

这一层只做"翻译"——把 HTTP 请求翻译成内部 Action 调用。

```java
public List<Route> routes() {
    return List.of(new Route(PUT, "/{index}/_ccr/follow"));
}

protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
    Request request = createRequest(restRequest);
    return channel -> client.execute(INSTANCE, request, new RestToXContentListener<>(channel));
}
```

REST Handler 不含任何业务逻辑，所有真正的活都在 Transport 层。

## 五、第二层：Transport 核心（TransportPutFollowAction）

### 继承关系

```java
public final class TransportPutFollowAction
    extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response>
```

继承 `TransportMasterNodeAction` → 操作必须在 master 节点执行（创建索引 = 修改集群状态）。

### 步骤 ① masterOperation — 验证

```java
protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
    // 1. 许可证检查
    if (ccrLicenseChecker.isCcrAllowed() == false) { ... }
    // 2. 验证远程集群可连接
    client.getRemoteClusterClient(remoteCluster, ...);
    // 3. 跨集群拉取 leader 索引元数据（异步）
    ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
        client, remoteCluster, leaderIndex,
        listener::onFailure,
        (historyUUID, tuple) -> createFollowerIndex(tuple.v1(), tuple.v2(), request, listener)
    );
}
```

| 检查 | 目的 |
|------|------|
| 本地许可证 | CCR 是付费功能 |
| 远程集群可连接 | remote_cluster 配置正确 |
| 拉取 leader 元数据 | 获取 leader 索引的 mapping/settings |

最后一个方法是异步的——拉取元数据是跨集群网络请求，用回调："等数据回来了，再调 `createFollowerIndex`"。

### 步骤 ② createFollowerIndex — 校验 + 伪装 restore

**前半：对 leader 索引做三道校验**

```java
if (leaderIndexMetadata == null) { ... }                    // leader 索引必须存在
if (INDEX_SOFT_DELETES_SETTING.get(...) == false) { ... }   // 必须开启 soft deletes
if (leaderIndexMetadata.isSearchableSnapshot()) { ... }     // 不能是 searchable snapshot
```

**后半：构造"伪装的" RestoreSnapshotRequest**

```java
final Settings overrideSettings = Settings.builder()
    .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)  // 标记为 follower 索引
    .put(request.getSettings())
    .build();

final RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(
    request.masterNodeTimeout(),
    leaderClusterRepoName,    // "_ccr_leader-cluster" (假仓库)
    CcrRepository.LATEST      // "_latest_" (假快照)
).indices(request.getLeaderIndex())
    .renamePattern("^(.*)$")
    .renameReplacement(request.getFollowerIndex())  // orders → orders-follower
    .indexSettings(overrideSettings)
    .quiet(true);

restoreService.restoreSnapshot(restoreRequest, listener, updater);
```

这就是 CCR 的核心设计——**把"初始数据引导"伪装成标准的 snapshot/restore 操作**：

| 参数 | 看起来是 | 实际是 |
|------|---------|--------|
| repository `_ccr_leader-cluster` | S3/文件系统快照仓库 | `CcrRepository`，一个假仓库，背后是远程集群连接 |
| snapshot `_latest_` | 某个具体快照 ID | 特殊标记，"给我 leader 此刻最新状态" |
| renameReplacement | 恢复时改个名 | `orders` → `orders-follower` |

为什么这么设计？ES 已有成熟的 restore 机制（创建索引、分配分片、传文件、失败重试），CCR 复用这套基础设施，不重新造轮子。

### 步骤 ③ afterRestoreStarted — 等 restore 完成

`restoreService.restoreSnapshot()` 返回时，只是在集群状态中"下了单"（创建索引元数据 + 分配分片指令）。实际的文件拉取是各节点异步执行的。

```java
private void afterRestoreStarted(Client clientWithHeaders, Request request,
    ActionListener<Response> originalListener, RestoreCompletionResponse response) {

    // 默认行为：不等，立即返回
    if (ActiveShardCount.NONE.equals(request.waitForActiveShards())) {
        originalListener.onResponse(new Response(true, false, false));
        listener = /* 只记日志的空壳 */;
    } else {
        listener = originalListener;
    }

    // 注册集群状态监听器，等 restore 真正完成
    RestoreClusterStateListener.createAndRegisterListener(clusterService, response,
        listener.delegateFailure((delegatedListener, restoreSnapshotResponse) -> {
            RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
            if (restoreInfo == null) {
                delegatedListener.onResponse(new Response(true, false, false));  // master 挂了
            } else if (restoreInfo.failedShards() == 0) {
                initiateFollowing(clientWithHeaders, request, delegatedListener); // 全部成功→启动复制
            } else {
                delegatedListener.onResponse(new Response(true, false, false));  // 有分片失败
            }
        }), ...);
}
```

只有 `failedShards == 0` 才启动复制——基础数据不完整就启动增量复制会导致数据损坏。

### 步骤 ④ initiateFollowing — 启动复制任务

```java
private void initiateFollowing(Client clientWithHeaders, Request request, ActionListener<Response> listener) {
    ResumeFollowAction.Request resumeFollowRequest = new ResumeFollowAction.Request(...);
    resumeFollowRequest.setFollowerIndex(request.getFollowerIndex());
    resumeFollowRequest.setParameters(new FollowParameters(parameters));
    clientWithHeaders.execute(ResumeFollowAction.INSTANCE, resumeFollowRequest,
        listener.delegateFailureAndWrap((l, r) ->
            ActiveShardsObserver.waitForActiveShards(...)
        ));
}
```

**PUT follow 的最后一步 = 调用 Resume Follow**。"开始复制"和"恢复复制"本质相同，都是创建 persistent task。

## 六、restoreService.restoreSnapshot() 内部做了什么

这是 **ES 框架已有的功能**，CCR 直接调用。

```
restoreService.restoreSnapshot(request, listener, updater)
    │
    │  阶段A: 准备（调 Repository 方法获取元数据）
    │    ① repositoriesService.repository("_ccr_xxx") → 找到 CcrRepository
    │    ② CcrRepository.getRepositoryData()          → 伪造仓库元数据
    │    ③ CcrRepository.getSnapshotInfo("_latest_")  → 伪造快照信息
    │    ④ CcrRepository.getSnapshotIndexMetaData()   → 去 Leader 拉索引元数据
    │
    │  阶段B: 修改集群状态（ClusterStateUpdateTask）
    │    ⑤ 创建索引元数据（含 CCR_FOLLOWING_INDEX_SETTING=true）
    │    ⑥ 在路由表添加分片，RecoverySource = SnapshotRecoverySource
    │    ⑦ 记录 RestoreInProgress（追踪恢复进度）
    │    ⑧ reroute → 分片分配到节点
    │
    │  集群状态发布 → listener.onResponse(RestoreCompletionResponse)
    │
    ▼  阶段C: 各节点异步执行（不在 restoreSnapshot 里）
    node-1 收到新集群状态
      → 看到 shard 0 的 RecoverySource 指向 "_ccr_xxx" 仓库
      → 调用 CcrRepository.restoreShard()
      → CcrRepository 跨集群拉 Lucene 文件 → 写入本地 Store
      → 分片变为 STARTED
```

## 七、哪些是 ES 框架已有的，哪些是 CCR 自己写的

### CCR 自己写的（如果你要自研类似功能，也要写）

| 组件 | 作用 |
|------|------|
| `RestPutFollowAction` | REST 入口，定义 URL 路由，解析参数 |
| `TransportPutFollowAction` | 业务编排：校验 → 构造 Request → 调 restore → 启动复制 |
| `CcrRepository` (implements Repository) | **核心工作量**：伪装仓库，实现 `restoreShard()` 去 Leader 拉数据 |
| Leader 端三个 RPC（Put/Get/Clear Session） | 被动提供文件字节 |
| `CcrLicenseChecker` | 许可证 + 远程元数据拉取 |

### ES 框架已有的（直接调用或继承）

| 组件 | 你怎么用 |
|------|---------|
| `TransportMasterNodeAction` | 继承，框架帮你路由到 master、重试 |
| `BaseRestHandler` | 继承，框架帮你处理 HTTP |
| `RestoreService.restoreSnapshot()` | 直接调用，框架创建索引/分配分片/触发恢复 |
| `RestoreClusterStateListener` | 直接调用，框架帮你等 restore 完成 |
| `ActiveShardsObserver` | 直接调用，框架帮你等分片激活 |
| `ActionListener.delegateFailure()` | 直接用，编排异步流程 |
| `RestoreSnapshotRequest` | 直接 new，填参数 |

### 关键关系

`CcrRepository` 是让 `restoreSnapshot()` 一行调用能跑通的**前提**。框架执行 restore 时会根据仓库名找到你注册的 Repository 实例，调用它的方法获取数据。没有 `CcrRepository`，框架找不到仓库，直接报错。

```
你写好 CcrRepository 注册进 ES（前提准备）
        ↓
TransportPutFollowAction 调用 restoreSnapshot("_ccr_xxx", "_latest_")（运行时触发）
        ↓
框架找到 CcrRepository，调其方法获取元数据和文件（框架驱动）
```

## 八、ActionListener 异步编排模式

ES 全异步，所有操作通过 `ActionListener` 回调串联。

### ActionListener 接口

```java
public interface ActionListener<Response> {
    void onResponse(Response response);   // 成功
    void onFailure(Exception e);          // 失败
}
```

### 链式工具方法

| 方法 | 含义 |
|------|------|
| `listener.delegateFailure((l, resp) -> ...)` | 成功走 lambda，失败自动转发给原 listener |
| `listener.delegateFailureAndWrap((l, resp) -> ...)` | 同上 + 额外 catch lambda 内异常 |
| `listener.map(resp -> transform(resp))` | 对结果做转换 |

### 本课的 listener 链路

```
masterOperation 的 listener (最终回复用户)
    │  检查失败 → listener.onFailure()
    │  拉取元数据成功 ↓
    │
    ├─ delegatelistener = listener.delegateFailure(→ afterRestoreStarted)
    │      │  restoreSnapshot 失败 → listener.onFailure()
    │      │  restoreSnapshot 成功 ↓
    │      │
    │      ├─ RestoreClusterStateListener（等集群状态变化）
    │      │      restore 失败 → Response(true, false, false)
    │      │      restore 成功 ↓
    │      │
    │      └─ initiateFollowing()
    │             │  ResumeFollowAction + ActiveShardsObserver
    │             ▼
    │         listener.onResponse(true, true, true)  ← 全部成功
```

每一层的 `delegateFailure` 保证：无论哪一步出错，异常都沿着链条自动回传给最外层的 listener。

### ES 中常用的监听/回调机制

| 机制 | 适用场景 |
|------|---------|
| `ActionListener<T>` | 任何异步操作等结果 |
| `ClusterStateListener` | 等集群状态满足某条件 |
| `ActiveShardsObserver` | 等指定索引的分片激活 |
| `PlainActionFuture<T>` | 把异步变同步（测试用，生产少用） |

## 九、小结

1. **REST 层只做翻译**，所有逻辑在 Transport 层。

2. **必须在 master 节点执行**（`TransportMasterNodeAction`），因为创建索引要改集群状态。

3. **三步走 = 验证 → restore → 启动复制**，对应 Response 的三个布尔值。

4. **核心设计：把"初始数据引导"伪装成 snapshot/restore**。CCR 在 Follower 上实现了 `CcrRepository`（假仓库），框架问它要数据时，它去 Leader 的 RPC 端点拉取。这样复用 ES 整套 restore 机制。

5. **PUT follow 的最后一步是调用 Resume Follow**——"开始"和"恢复"复制本质相同。

6. **两个前置条件**：leader 必须开 soft deletes（第 2 课），follower 标记 `CCR_FOLLOWING_INDEX_SETTING=true` 从而使用 FollowingEngine（第 18 课）。

7. **要自研类似功能，核心工作量是实现 Repository 接口**（特别是 `restoreShard()`），然后构造 `RestoreSnapshotRequest` 调用 `restoreService.restoreSnapshot()` 触发流程。

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/rest/RestPutFollowAction.java` — REST 入口
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/TransportPutFollowAction.java` — 核心业务编排
- `x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/ccr/action/PutFollowAction.java` — Request/Response 定义
- `server/src/main/java/org/elasticsearch/snapshots/RestoreService.java` — ES 框架的 restore 实现（直接调用）
