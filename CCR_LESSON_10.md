# 第 10 课：CcrRepository — 基于 Snapshot/Restore 的数据引导

## 一、核心问题

第 9 课我们知道 CCR 把初始数据引导伪装成 snapshot/restore。这节课深入回答：

> CcrRepository 这个"假仓库"具体怎么骗过框架的？Leader 端怎么提供文件？文件怎么传输？Follower 和 Leader 各自执行了什么？

## 二、为什么用 Snapshot/Restore 而不是重放 Translog

| 方案 | 问题 |
|------|------|
| 重放全部 translog | translog 早就截断了，历史操作不存在 |
| 重放 soft deletes 保留的操作 | 只保留近期的，远古操作被 merge 丢了 |
| 拷贝 Lucene segment 文件 | **唯一可行方案**——复制"此刻的数据快照" |

CCR 的 bootstrap = 把 leader 当前时刻的 Lucene 文件原封不动搬到 follower。这和 restore 做的事完全一样。

## 三、CcrRepository 的设计总览

### 类定义

```java
public class CcrRepository extends AbstractLifecycleComponent implements Repository {
    public static final String LATEST = "_latest_";   // 伪造的快照ID
    public static final String TYPE = "_ccr_";        // 仓库类型
    public static final String NAME_PREFIX = "_ccr_"; // 仓库名前缀
```

### 为什么继承 AbstractLifecycleComponent

Repository 是长期存活的对象（跟 ES 进程同生共死），框架通过 `doStart()`/`doStop()`/`doClose()` 管理其生命周期。CcrRepository 没有需要手动管理的资源，所以这三个方法都是空的，但必须继承以满足框架要求。

### 构造函数

```java
public CcrRepository(RepositoryMetadata metadata, Client client, Settings settings, ...) {
    // 从仓库名解析出远程集群别名: "_ccr_leader-cluster" → "leader-cluster"
    this.remoteClusterAlias = Strings.split(metadata.name(), NAME_PREFIX)[1];
    this.client = client;
    // 去重器：多个并发调用只发一次 RPC
    csDeduplicator = new SingleResultDeduplicator<>(
        threadPool.getThreadContext(),
        l -> getRemoteClusterClient().execute(
            ClusterStateAction.REMOTE_TYPE,  // → Leader RPC
            new ClusterStateRequest(TimeValue.MAX_VALUE).clear().metadata(true).nodes(true),
            l.map(ClusterStateResponse::getState)
        )
    );
}
```

### 方法分类

| 类别 | 方法 | 作用 |
|------|------|------|
| 元数据伪造 | `getRepositoryData()` | 告诉框架"仓库有什么快照" |
| 元数据伪造 | `getSnapshotInfo()` | 告诉框架"快照状态和版本" |
| 真实拉取 | `getSnapshotIndexMetaData()` | 从 leader 拉索引结构 |
| 真实拉取 | `getShardSnapshotStatus()` | 从 leader 查分片大小 |
| 核心搬运 | `restoreShard()` | 从 leader 拉 Lucene 文件写入本地 |
| 拒绝服务 | `finalizeSnapshot()`/`snapshotShard()` 等 | 抛异常，只读仓库 |
| 生命周期 | `doStart()`/`doStop()`/`doClose()` | 空实现 |

## 四、按执行顺序讲解每个方法

---

### 第一步：`getRepositoryData()`

**谁调的**：Follower 的 `RestoreService`

**执行位置**：Follower 节点

**目的**：框架问"你这仓库有什么快照、包含哪些索引？"

```java
@Override
public void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener) {
    csDeduplicator.execute(new ThreadedActionListener<>(responseExecutor, listener.map(response -> {
        // response = Leader 的集群状态（通过 RPC 拿到的）
        final Metadata remoteMetadata = response.getMetadata();
        final String[] concreteAllIndices = remoteMetadata.getConcreteAllIndices();

        final Map<IndexId, List<SnapshotId>> indexSnapshots = Maps.newMapWithExpectedSize(...);
        for (String indexName : concreteAllIndices) {
            // 伪造：每个 leader 索引 = 一个名为 "_latest_" 的快照
            final SnapshotId snapshotId = new SnapshotId(LATEST, LATEST);
            indexSnapshots.put(
                new IndexId(indexName, remoteIndices.get(indexName).getIndex().getUUID()),
                List.of(snapshotId)
            );
        }
        return new RepositoryData(..., indexSnapshots, ...);
    })));
}
```

| 操作 | 执行位置 | 说明 |
|------|---------|------|
| `csDeduplicator` 内部发 `ClusterStateAction` | **Follower → Leader RPC** | 拉取 leader 全部索引列表 |
| 遍历索引，构造 `RepositoryData` | **Follower 本地** | 伪装成快照仓库格式 |

---

### 第二步：`getSnapshotInfo()`

**谁调的**：Follower 的 `RestoreService`

**执行位置**：Follower 节点

**目的**：框架问"这个快照状态正常吗？版本兼容吗？"

```java
@Override
public void getSnapshotInfo(...) {
    csDeduplicator.execute(...listener.map(response -> {
        // 版本检查：leader 不能比 follower 版本高
        IndexVersion maxIndexVersion = response.getNodes().getMaxDataNodeCompatibleIndexVersion();
        for (var node : response.nodes()) {
            if (node.canContainData() && node.getBuildVersion().isFutureVersion()) {
                throw new SnapshotException(..., "version higher than this node");
            }
        }

        // 伪造 SnapshotInfo
        consumer.accept(new SnapshotInfo(
            snapshot,
            List.copyOf(responseMetadata.indices().keySet()),  // leader 所有索引名
            ...,
            SnapshotState.SUCCESS  // 状态永远是成功
        ));
        return null;
    }));
}
```

| 操作 | 执行位置 | 说明 |
|------|---------|------|
| `csDeduplicator` | **Follower → Leader RPC**（或复用缓存） | 复用第一步的集群状态 |
| 版本检查 | **Follower 本地** | leader 版本不能高于 follower |
| 构造 `SnapshotInfo` | **Follower 本地** | 伪造状态 = SUCCESS |

---

### 第三步：`getSnapshotIndexMetaData()`

**谁调的**：Follower 的 `RestoreService.startRestore()`

**执行位置**：Follower 节点

**目的**：框架问"我要建索引了，告诉我 mapping、settings、分片数"

```java
@Override
public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
    String leaderIndex = index.getName();
    var remoteClient = getRemoteClusterClient();

    // ① 拉取 leader 上该索引的集群状态
    ClusterStateResponse clusterState = executeRecoveryAction(
        remoteClient, ClusterStateAction.REMOTE_TYPE, CcrRequests.metadataRequest(leaderIndex));

    IndexMetadata leaderIndexMetadata = clusterState.getState().metadata().index(leaderIndex);

    // ② 拉取 history UUIDs
    CcrLicenseChecker.fetchLeaderHistoryUUIDs(remoteClient, leaderIndexMetadata, ...);
    String[] leaderHistoryUUIDs = future.actionGet(...);

    // ③ 构造 IndexMetadata，嵌入 CCR 自定义信息
    IndexMetadata.Builder imdBuilder = IndexMetadata.builder(leaderIndex);

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put(CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY, leaderIndexMetadata.getIndexUUID());
    customMetadata.put(CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY, leaderIndex);
    customMetadata.put(CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY, remoteClusterAlias);
    customMetadata.put(CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS, String.join(",", leaderHistoryUUIDs));
    imdBuilder.putCustom(Ccr.CCR_CUSTOM_METADATA_KEY, customMetadata);

    imdBuilder.settings(leaderIndexMetadata.getSettings());
    imdBuilder.putMapping(leaderIndexMetadata.mapping());
    imdBuilder.setRoutingNumShards(leaderIndexMetadata.getRoutingNumShards());

    // 伪造 in-sync allocation IDs，让分片可以被分配
    for (var key : leaderIndexMetadata.getInSyncAllocationIds().keySet()) {
        imdBuilder.putInSyncAllocationIds(key, Collections.singleton(IN_SYNC_ALLOCATION_ID));
    }
    return imdBuilder.build();
}
```

| 操作 | 执行位置 | 说明 |
|------|---------|------|
| `ClusterStateAction` 拉指定索引 | **Follower → Leader RPC** | 获取 leader 索引的元数据 |
| `fetchLeaderHistoryUUIDs` | **Follower → Leader RPC** | 获取每个分片的 history UUID |
| 构造 `customMetadata` | **Follower 本地** | 塞入 leader 信息，传递给后续 `restoreShard()` |
| 复制 settings/mapping/routing | **Follower 本地** | follower 继承 leader 的索引结构 |
| 伪造 `inSyncAllocationIds` | **Follower 本地** | 骗过分片分配器 |

**重要**：`customMetadata` 里塞的 leader 信息，后面 `restoreShard()` 会从中取出来使用——这是跨方法传递信息的机制。

---

### 第四步：`getShardSnapshotStatus()`

**谁调的**：Follower 的分片分配决策

**执行位置**：Follower 节点

**目的**：框架问"这个分片数据有多大？我好决定放到哪个节点"

```java
@Override
public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId index, ShardId shardId) {
    final String leaderIndex = index.getName();
    // 去 leader 查 index stats
    final IndicesStatsResponse response = executeRecoveryAction(
        getRemoteClusterClient(), IndicesStatsAction.REMOTE_TYPE,
        new IndicesStatsRequest().indices(leaderIndex).clear().store(true));

    // 找到对应主分片的大小
    for (ShardStats shardStats : response.getIndex(leaderIndex).getShards()) {
        final ShardRouting shardRouting = shardStats.getShardRouting();
        if (shardRouting.shardId().id() == shardId.getId() && shardRouting.primary() && shardRouting.active()) {
            final long totalSize = shardStats.getStats().getStore().sizeInBytes();
            return IndexShardSnapshotStatus.newDone(0L, 0L, 1, 1, totalSize, totalSize, DUMMY_GENERATION);
        }
    }
    throw new ElasticsearchException("Could not get shard stats ...");
}
```

| 操作 | 执行位置 | 说明 |
|------|---------|------|
| `IndicesStatsAction` | **Follower → Leader RPC** | 查询 leader 分片的 store 大小 |
| 遍历找主分片大小 | **Follower 本地** | 匹配 shard id + primary + active |

---

### ——框架自己干活的分界线——

到这里，框架拿到全部信息后**自己**做：
1. 修改集群状态，创建索引
2. 分配分片到节点
3. 发布新集群状态

各节点收到后启动恢复流程，调用下面的方法。

---

### 第五步：`restoreShard()` — 核心方法

**谁调的**：Follower 数据节点的 `IndexShard` 恢复流程

**执行位置**：Follower 数据节点

**目的**：把 leader 分片的 Lucene 文件搬到 follower 本地

#### 5.1 清空本地存储（Follower 本地）

```java
createEmptyStore(store);  // store.createEmpty() 清空 Lucene 目录
```

#### 5.2 从元数据读取 leader 信息（Follower 本地）

```java
final Map<String, String> ccrMetadata = store.indexSettings().getIndexMetadata()
    .getCustomData(Ccr.CCR_CUSTOM_METADATA_KEY);
final String leaderIndexName = ccrMetadata.get(CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY);
final String leaderUUID = ccrMetadata.get(CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY);
final Index leaderIndex = new Index(leaderIndexName, leaderUUID);
final ShardId leaderShardId = new ShardId(leaderIndex, shardId.getId());
```

这就是第三步塞的 `customMetadata` 的用处——跨方法传递 leader 信息。

#### 5.3 注册 retention lease（Follower → Leader RPC）

```java
acquireRetentionLeaseOnLeader(shardId, retentionLeaseId, leaderShardId, remoteClient);
```

内部逻辑：先 add → 已存在则 renew → renew 时不存在则再 add。`RETAIN_ALL` 表示从 seq_no 0 全部保留。

| 操作 | 执行位置 | 说明 |
|------|---------|------|
| `syncAddRetentionLease` | **Follower → Leader RPC** | 在 leader 上注册 retention lease |
| `syncRenewRetentionLease` | **Follower → Leader RPC** | 续期 |

#### 5.4 启动 lease 定期续期（Follower 定时器 + 定期 → Leader RPC）

```java
final Scheduler.Cancellable renewable = threadPool.scheduleWithFixedDelay(() -> {
    CcrRetentionLeases.asyncRenewRetentionLease(leaderShardId, retentionLeaseId, RETAIN_ALL, remoteClient, ...);
}, RETENTION_LEASE_RENEW_INTERVAL, ...);
```

每 30 秒向 leader 续期一次，防止长时间 restore 导致 lease 过期。

#### 5.5 打开文件会话（Follower → Leader RPC）

```java
openSession(metadata.name(), remoteClient, leaderShardId, shardId, recoveryState, sessionListener);
```

Follower 发送 `PutCcrRestoreSessionAction` 给 Leader：

```java
void openSession(..., ActionListener<RestoreSession> listener) {
    String sessionUUID = UUIDs.randomBase64UUID();
    remoteClient.execute(
        PutCcrRestoreSessionAction.REMOTE_INTERNAL_TYPE,
        new PutCcrRestoreSessionRequest(sessionUUID, leaderShardId),
        responseListener  // 响应包含：文件列表 + 处理节点
    );
}
```

**Leader 收到后的完整调用链**：

```
Follower: remoteClient.execute(PutCcrRestoreSessionAction, request)
    │ 网络传输
    ▼
Leader Transport 层: 根据 action name 找到注册的 handler
    │ (Ccr.java 启动时注册: action name → InternalTransportAction.class)
    ▼
TransportPutCcrRestoreSessionAction (extends TransportSingleShardAction)
    │ 框架自动路由到 leader 分片的主副本所在节点
    ▼
shardOperation(request, shardId):
    │ IndexShard indexShard = indicesService.getShardOrNull(shardId);
    │ Store.MetadataSnapshot metadata = ccrRestoreService.openSession(sessionUUID, indexShard);
    │ long mappingVersion = indexShard.indexSettings()...getMappingVersion();
    │ return new PutCcrRestoreSessionResponse(localNode, metadata, mappingVersion);
    ▼
CcrRestoreSourceService.openSession():
    │ Engine.IndexCommitRef commitRef = indexShard.acquireSafeIndexCommit();  // 锁住文件
    │ Set<String> fileNames = commitRef.getIndexCommit().getFileNames();
    │ new RestoreSession(sessionUUID, indexShard, commitRef, fileNames, scheduleTimeout(...));
    │ return store.getMetadata(commitRef);  // 返回文件元数据
    ▼
响应回到 Follower: 收到文件列表（每个文件的名字、大小、校验和）
```

**RPC 路由的关键**：`TransportSingleShardAction` 框架会根据 `shards()` 方法自动把请求路由到分片的主副本所在节点：

```java
@Override
protected ShardsIterator shards(ClusterState state, InternalRequest request) {
    return state.routingTable().shardRoutingTable(shardId).primaryShardIt();
}
```

#### 5.6 分块拉取文件（循环 Follower → Leader RPC）

```java
void restoreFiles(Store store, ActionListener<Void> listener) {
    // 把文件元数据转成 FileInfo 列表
    for (StoreFileMetadata fileMetadata : sourceMetadata) {
        fileInfos.add(new FileInfo(fileMetadata.name(), fileMetadata, ...));
    }
    restore(snapshotFiles, store, listener);  // 进入分块传输
}
```

实际传输使用 ES 框架的 `MultiChunkTransfer`：

```java
final MultiChunkTransfer<StoreFileMetadata, FileChunk> multiFileTransfer = new MultiChunkTransfer<>(
    ..., ccrSettings.getMaxConcurrentFileChunks(), mds  // 并发5，文件列表
) {
    final MultiFileWriter multiFileWriter = new MultiFileWriter(store, ...);
    long offset = 0;

    @Override
    protected void onNewResource(StoreFileMetadata md) {
        offset = 0;  // 新文件，偏移归零
    }

    @Override
    protected FileChunk nextChunkRequest(StoreFileMetadata md) {
        // [Follower 本地] 计算这次要多少字节
        int bytesRequested = Math.min(ccrSettings.getChunkSize(), md.length() - offset);
        offset += bytesRequested;
        return new FileChunk(md, bytesRequested, offset == md.length());
    }

    @Override
    protected void executeChunkRequest(FileChunk request, ActionListener<Void> listener) {
        // [Follower → Leader RPC] 拉取一块
        remoteClient.execute(
            GetCcrRestoreFileChunkAction.REMOTE_INTERNAL_TYPE,
            new GetCcrRestoreFileChunkRequest(node, sessionUUID, request.md.name(), request.bytesRequested, leaderShardId),
            listener.map(response -> {
                writeFileChunk(request.md, response);  // [Follower 本地] 写入
                return null;
            })
        );
    }

    private void writeFileChunk(StoreFileMetadata md, GetCcrRestoreFileChunkResponse r) {
        // [Follower 本地] 限速
        ccrSettings.getRateLimiter().maybePause(actualChunkSize);
        // [Follower 本地] 写入磁盘
        multiFileWriter.writeFileChunk(md, r.getOffset(), r.getChunk(), lastChunk);
    }
};
multiFileTransfer.start();
```

**Leader 收到 `GetCcrRestoreFileChunkAction` 后**（`CcrRestoreSourceService.readFileBytes()`）：

```java
private long readFileBytes(String fileName, ByteArray reference) throws IOException {
    try (Releasable ignored = keyedLock.acquire(fileName)) {
        // 打开或复用缓存的文件句柄
        final IndexInput indexInput = cachedInputs.computeIfAbsent(fileName, f ->
            commitRef.getIndexCommit().getDirectory().openInput(fileName, context));
        // 读取字节
        reference.fillWith(new InputStreamIndexInput(indexInput, reference.size()));
        long offsetAfterRead = indexInput.getFilePointer();
        // 文件读完后关闭句柄
        if (offsetAfterRead == indexInput.length()) {
            cachedInputs.remove(fileName);
            IOUtils.close(indexInput);
        }
        return offsetAfterRead;
    }
}
```

| 操作 | 执行位置 | 说明 |
|------|---------|------|
| `nextChunkRequest()` | **Follower 本地** | 计算偏移和大小 |
| `GetCcrRestoreFileChunkAction` | **Follower → Leader RPC** | 拉取文件块 |
| `readFileBytes()` | **Leader 本地** | 从 Lucene 目录读字节返回 |
| `getRateLimiter().maybePause()` | **Follower 本地** | 限速 40MB/s |
| `multiFileWriter.writeFileChunk()` | **Follower 本地** | 写入磁盘 |

传输参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `maxConcurrentFileChunks` | 5 | 最多同时拉 5 个块 |
| `chunkSize` | 1MB | 每块大小 |
| `rateLimiter` | 40MB/s | 传输限速 |

#### 5.7 更新 mapping（Follower → Leader RPC + Follower 本地）

```java
updateMappings(remoteClient, leaderIndex, restoreSession.mappingVersion, client, shardId.getIndex());
```

| 操作 | 执行位置 | 说明 |
|------|---------|------|
| `getIndexMetadata(leaderClient, ...)` | **Follower → Leader RPC** | 拉取 leader 最新 mapping |
| `putMapping(...)` | **Follower 本地** | 更新 follower 索引的 mapping |

为什么需要：文件传输可能花几分钟，期间 leader mapping 可能变了（加了新字段）。

#### 5.8 关闭会话（Follower → Leader RPC）

```java
public void close(ActionListener<Void> listener) {
    remoteClient.execute(ClearCcrRestoreSessionAction.REMOTE_INTERNAL_TYPE,
        new ClearCcrRestoreSessionRequest(sessionUUID, node, leaderShardId), ...);
}
```

**Leader 收到后**（`CcrRestoreSourceService.closeSession()`）：
1. 从 `onGoingRestores` map 移除会话
2. 取消超时定时器
3. 关闭所有 `IndexInput` 文件句柄
4. 释放 `IndexCommitRef`（解锁文件，merge 可以继续）

---

## 五、Leader 端：CcrRestoreSourceService

运行在 Leader 节点上，职责是管理文件会话。

### 核心数据结构

```java
public class CcrRestoreSourceService extends AbstractLifecycleComponent implements IndexEventListener {
    private final Map<String, RestoreSession> onGoingRestores = ConcurrentCollections.newConcurrentMap();
    private final Map<IndexShard, HashSet<String>> sessionsForShard = new HashMap<>();
```

- `onGoingRestores`：sessionUUID → 会话对象
- `sessionsForShard`：shard → 该 shard 上的会话列表（用于 shard 关闭时清理）

### `openSession()` — Leader 执行

```java
public synchronized Store.MetadataSnapshot openSession(String sessionUUID, IndexShard indexShard) {
    // 锁住当前 Lucene commit（防止 merge 删除文件）
    final Engine.IndexCommitRef commitRef = indexShard.acquireSafeIndexCommit();
    final Set<String> fileNames = Set.copyOf(commitRef.getIndexCommit().getFileNames());
    // 创建会话 + 启动超时检测
    restore = new RestoreSession(sessionUUID, indexShard, commitRef, fileNames, scheduleTimeout(sessionUUID));
    onGoingRestores.put(sessionUUID, restore);
    // 返回文件元数据
    return restore.getMetadata();
}
```

`acquireSafeIndexCommit()` 是关键——有这个引用在，Lucene merge 不会删除 segment 文件。

### 超时机制 — Leader 执行

```java
private Scheduler.Cancellable scheduleTimeout(String sessionUUID) {
    return threadPool.scheduleWithFixedDelay(() -> maybeTimeout(sessionUUID), idleTimeout, ...);
}

private void maybeTimeout(String sessionUUID) {
    if (restoreSession.idle) {
        internalCloseSession(sessionUUID, false);  // 空闲太久，关闭
    } else {
        restoreSession.idle = true;  // 标记空闲，下次检查如果还是空闲就关
    }
}
```

两次检查机制：每次 getFileChunk 请求会重置 `idle=false`，如果连续两个超时周期（默认 60s）没有请求，会话自动关闭。

### shard 关闭时清理 — Leader 执行

```java
public synchronized void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings settings) {
    HashSet<String> sessions = sessionsForShard.remove(indexShard);
    if (sessions != null) {
        for (String sessionUUID : sessions) {
            onGoingRestores.remove(sessionUUID).decRef();  // 释放 commit 引用
        }
    }
}
```

如果 Leader 的 shard 被关闭（比如节点下线），所有关联会话自动清理。

---

## 六、RPC 路由机制——Follower 怎么"进入"Leader 执行

Follower 代码里只有 `remoteClient.execute(action, request, listener)`，看不到 Leader 的函数调用。中间靠 **Transport Action 的名字注册路由**连接。

### 注册（Leader 启动时）

`Ccr.java` 的 `getActions()` 注册了：
```java
new ActionHandler<>(PutCcrRestoreSessionAction.INTERNAL_INSTANCE, 
                    PutCcrRestoreSessionAction.InternalTransportAction.class)
```

这告诉 Transport 层："收到 `internal:admin/ccr/restore/session/put` 的请求，用 `InternalTransportAction` 处理。"

### 传递链

```
PutCcrRestoreSessionAction.INTERNAL_NAME = "internal:admin/ccr/restore/session/put"
    ↓ InternalTransportAction 构造函数 super(INTERNAL_NAME, ...)
TransportPutCcrRestoreSessionAction(actionName, ...)
    ↓ super(actionName, ...)
TransportSingleShardAction(actionName, ...)
    ↓ super(actionName, ...)
TransportAction → transportService.registerRequestHandler(actionName, handler)
```

### 运行时调用链

```
Follower: remoteClient.execute("internal:admin/ccr/restore/session/put", request)
    │ 网络
    ▼
Leader Transport 层: 根据 action name 路由到 InternalTransportAction
    │ TransportSingleShardAction 框架自动路由到主分片所在节点
    ▼
shardOperation(request, shardId):
    indexShard = indicesService.getShardOrNull(shardId)
    ccrRestoreService.openSession(sessionUUID, indexShard)  ← 这里调到了 openSession
    return Response(localNode, fileMetadata, mappingVersion)
    │ 网络
    ▼
Follower: listener.onResponse(response)
```

### TransportSingleShardAction vs TransportMasterNodeAction

| | `TransportMasterNodeAction` | `TransportSingleShardAction` |
|---|---|---|
| 路由到 | Master 节点 | 指定分片的主副本所在节点 |
| 你实现 | `masterOperation()` | `shardOperation()` |
| 适用场景 | 修改集群状态 | 操作某个具体分片 |
| CCR 中的例子 | `TransportPutFollowAction` | `PutCcrRestoreSessionAction` |

---

## 七、Retention Lease 在 restore 中的作用

`restoreShard()` 开头就注册了 retention lease：

- **为什么**：虽然 `acquireSafeIndexCommit()` 锁住了当前文件，但 restore 完成后到增量复制启动前这段时间，leader 可能 merge 掉中间产生的操作。retention lease 保证从 restore 那一刻起的所有后续操作都不会丢。
- **续期**：restore 可能耗时很长，所以启动定时器每 30s 续期一次。
- **`RETAIN_ALL`**：从 seq_no 0 全部保留（最保守策略，restore 期间不知道确切起始点）。

---

## 八、完整时序图

```
Follower 节点                                    Leader 节点
──────────────                                  ──────────────

[RestoreService 调用 CcrRepository]

1. getRepositoryData()
   │── ClusterStateAction ──────── RPC ─────→  返回集群状态
   │← 伪造 RepositoryData 返回给框架

2. getSnapshotInfo()
   │── (复用缓存) ──────────────────────────→  (不发 RPC)
   │← 伪造 SnapshotInfo 返回给框架

3. getSnapshotIndexMetaData()
   │── ClusterStateAction ──────── RPC ─────→  返回索引元数据
   │── fetchHistoryUUIDs ────────── RPC ─────→  返回 history UUIDs
   │← 构造 IndexMetadata（含 CCR 自定义信息）返回给框架

4. getShardSnapshotStatus()
   │── IndicesStatsAction ────────── RPC ────→  返回分片大小
   │← 返回给框架用于分片分配

[框架自己：创建索引 → 分配分片 → 发布集群状态]

5. restoreShard()  （各数据节点执行）
   │
   ├─ 5.1 createEmptyStore()                    [Follower 本地]
   │
   ├─ 5.2 读取 CCR custom metadata             [Follower 本地]
   │
   ├─ 5.3 acquireRetentionLease ──── RPC ─────→ 注册 retention lease
   │
   ├─ 5.4 scheduleWithFixedDelay               [Follower 本地]
   │         └─ renewLease ──────── RPC ─────→  续期（每30s）
   │
   ├─ 5.5 PutCcrRestoreSession ──── RPC ─────→ openSession()
   │                                             acquireSafeIndexCommit()
   │                                             返回文件列表
   │
   ├─ 5.6 GetCcrRestoreFileChunk ── RPC ─────→ readFileBytes() 返回字节
   │       GetCcrRestoreFileChunk ── RPC ─────→ readFileBytes()
   │       ... (每文件每块一次, 5并发)            ...
   │       writeFileChunk()                     [Follower 本地] 写磁盘
   │
   ├─ 5.7 getIndexMetadata ──────── RPC ─────→ 返回最新 mapping
   │       putMapping()                         [Follower 本地]
   │
   └─ 5.8 ClearCcrRestoreSession ── RPC ─────→ closeSession()
                                                 释放 commitRef + 关文件句柄
```

---

## 九、哪些是 ES 框架，哪些是 CCR 自己写的

### CCR 自己写的（Follower 端）

| 组件 | 作用 |
|------|------|
| `CcrRepository` (implements Repository) | 假仓库：伪造元数据 + 实现 restoreShard |

### CCR 自己写的（Leader 端）

| 组件 | 作用 |
|------|------|
| `CcrRestoreSourceService` | 管理文件会话：开/关/超时/读文件 |
| `PutCcrRestoreSessionAction` + Transport 类 | RPC 端点：打开会话 |
| `GetCcrRestoreFileChunkAction` + Transport 类 | RPC 端点：拉文件块 |
| `ClearCcrRestoreSessionAction` + Transport 类 | RPC 端点：关闭会话 |

### ES 框架已有的

| 组件 | CCR 怎么用 |
|------|-----------|
| `RestoreService` | 直接调用，创建索引/分配分片/触发恢复 |
| `TransportSingleShardAction` | Leader Action 继承它，框架自动路由到分片所在节点 |
| `MultiChunkTransfer` | Follower 用，并发分块传输工具 |
| `MultiFileWriter` | Follower 用，写入本地 Store |
| `Store` / `IndexInput` / `IndexCommit` | Lucene 层文件操作 |
| `SingleResultDeduplicator` | 合并并发 RPC 调用 |

---

## 十、小结

1. **CcrRepository 运行在 Follower 上**，伪装成快照仓库骗过 `RestoreService`。它内部通过 RPC 去 Leader 拉取数据。

2. **Leader 只是被动的文件服务器**：三个 Transport Action 提供"开会话/读文件/关会话"的 RPC 端点，内部由 `CcrRestoreSourceService` 管理。

3. **RPC 路由靠 action name 注册**：`Ccr.java` 启动时注册 action name → handler 映射；Follower 发 RPC 时只指定 action name；Leader Transport 层按名字找到 handler，调用 `shardOperation()`，最终调到 `CcrRestoreSourceService.openSession()`。

4. **文件传输是分块并发的**：默认 5 并发、每块 1MB、限速 40MB/s。一个 10GB 的分片需要约 10000 次 RPC。

5. **Retention lease 从 restore 开始就注册**，保证 restore 完成后增量复制能无缝接上。

6. **`acquireSafeIndexCommit()`** 是 Leader 端的关键操作——锁住 Lucene 文件不被 merge 删除，直到会话关闭。

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/repository/CcrRepository.java` — Follower 端假仓库
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/repository/CcrRestoreSourceService.java` — Leader 端文件会话管理
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/repositories/PutCcrRestoreSessionAction.java` — 打开会话的 RPC Action
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/repositories/GetCcrRestoreFileChunkAction.java` — 拉文件块的 RPC Action
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/repositories/ClearCcrRestoreSessionAction.java` — 关闭会话的 RPC Action
