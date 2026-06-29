# 第 11 课：CcrRepositoryManager 与分片分配

## 一、核心问题

上一课知道 `CcrRepository` 是个"假仓库"，RestoreService 通过仓库名找到它。那么：

1. **谁负责把 CcrRepository 注册进 ES？** 什么时候注册、什么时候删除？
2. **Follower 的主分片可以分配到任意节点吗？** 如果不行，怎么限制？

---

## 二、执行顺序详解

### 阶段一：节点启动，创建 CcrRepositoryManager

ES 节点启动，加载 CCR 插件，调用 `Ccr.java` 的 `createComponents()`：

```java
new CcrRepositoryManager(settings, services.clusterService(), client)
```

进入构造函数：

```java
CcrRepositoryManager(Settings settings, ClusterService clusterService, Client client) {
    this.client = client;
    updateListener = new RemoteSettingsUpdateListener(settings);
    updateListener.listenForUpdates(clusterService.getClusterSettings());
}
```

做了两件事：
1. 创建 `RemoteSettingsUpdateListener`（继承 ES 框架的 `RemoteClusterAware`），把当前 settings 存下来
2. `listenForUpdates()`——ES 框架方法，把所有远程集群相关 Setting（seeds、proxy、mode 等）注册成一组监听器

此时还没注册任何 Repository，只是"挂好耳朵准备听"。

---

### 阶段二：组件启动，init() 注册已有远程集群

ES 框架创建完所有组件后调用 `doStart()`：

```java
@Override
protected void doStart() {
    updateListener.init();
}
```

进入 `init()`：

```java
void init() {
    Set<String> clusterAliases = getEnabledRemoteClusters(settings);
    for (String clusterAlias : clusterAliases) {
        putRepository(CcrRepository.NAME_PREFIX + clusterAlias);
    }
}
```

`getEnabledRemoteClusters(settings)` 从 settings 中提取所有已配置的远程集群名。例如配置了：

```yaml
cluster.remote.leader-cluster.seeds: ["10.0.0.1:9300"]
cluster.remote.us-west.seeds: ["10.0.0.2:9300"]
```

返回 `{"leader-cluster", "us-west"}`，然后对每个调用 `putRepository("_ccr_leader-cluster")`、`putRepository("_ccr_us-west")`。

---

### 阶段三：putRepository 执行链

```java
private void putRepository(String repositoryName) {
    ActionRequest request = new PutInternalCcrRepositoryRequest(repositoryName, CcrRepository.TYPE);
    PlainActionFuture<ActionResponse.Empty> f = new PlainActionFuture<>();
    client.execute(PutInternalCcrRepositoryAction.INSTANCE, request, f);
}
```

通过 `client.execute()` 执行本地 Action（不走网络），处理类只做一件事：

```java
// TransportPutInternalRepositoryAction.doExecute()
repositoriesService.registerInternalRepository(request.getName(), request.getType());
listener.onResponse(ActionResponse.Empty.INSTANCE);
```

`registerInternalRepository("_ccr_leader-cluster", "_ccr_")` 在 RepositoriesService 内部的 Map 中记录仓库名和类型。之后 RestoreService 按名字查找就能找到对应的 CcrRepository 实例。

---

### 阶段四：运行时动态添加远程集群

管理员执行：

```
PUT _cluster/settings
{ "persistent": { "cluster.remote.new-dc.seeds": ["10.0.0.3:9300"] } }
```

ES 框架更新设置 → 触发变更事件 → ClusterSettings 通知监听器 → 回调：

```java
@Override
protected void updateRemoteCluster(String clusterAlias, Settings settings) {
    String repositoryName = CcrRepository.NAME_PREFIX + clusterAlias;
    if (RemoteConnectionStrategy.isConnectionEnabled(clusterAlias, settings)) {
        putRepository(repositoryName);     // 启用 → 注册
    } else {
        deleteRepository(repositoryName);  // 禁用 → 删除
    }
}
```

动态增删远程集群配置时，对应的 CCR Repository 随之增删。

---

### 阶段五：用户 PUT follow，仓库已就位

`TransportPutFollowAction.createFollowerIndex()` 构造 `RestoreSnapshotRequest`，仓库名 `"_ccr_leader-cluster"`。

`RestoreService.restoreSnapshot()` 内部：

```java
Repository repository = repositoriesService.repository("_ccr_leader-cluster");
```

因为阶段三已经注册过，直接找到 CcrRepository 实例。**如果没有 CcrRepositoryManager 提前注册，这里会抛 RepositoryMissingException。**

---

### 阶段六：分片分配，AllocationDecider 介入

RestoreService 在集群状态中创建索引 + 分片路由后，触发 reroute。

分配器对每个 (分片, 节点) 组合依次询问所有 AllocationDecider。当询问到 `CcrPrimaryFollowerAllocationDecider.canAllocate()` 时：

**第 1 步：是不是 follower 索引？**

```java
if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(indexMetadata.getSettings()) == false) {
    return Decision.YES;  // 不是 follower，放行
}
```

**第 2 步：是不是主分片？**

```java
if (shardRouting.primary() == false) {
    return Decision.YES;  // 副本不需要跨集群拉数据，放行
}
```

**第 3 步：是不是正在 bootstrap？**

```java
final RecoverySource recoverySource = shardRouting.recoverySource();
if (recoverySource == null || recoverySource.getType() != RecoverySource.Type.SNAPSHOT) {
    return Decision.YES;  // 已经 bootstrap 完了，放行
}
```

只有 RecoverySource 是 SNAPSHOT 类型，才说明分片还在初始恢复阶段（需要通过 CcrRepository 从 Leader 拉文件）。

**第 4 步：目标节点有没有 remote_cluster_client 角色？**

```java
if (node.node().isRemoteClusterClient() == false) {
    return Decision.NO;   // 没有远程连接能力，拒绝
}
return Decision.YES;      // 有，允许
```

---

## 三、整体时间线

```
节点启动
  │
  ├─ Ccr.createComponents() → new CcrRepositoryManager()
  │     └─ listenForUpdates() 注册配置监听
  │
  ├─ 框架调用 doStart()
  │     └─ init() → 遍历已有远程集群 → putRepository("_ccr_xxx") 逐个注册
  │
  ▼ 节点运行中...
  │
  ├─ [可选] 管理员动态添加/删除远程集群
  │     └─ updateRemoteCluster() 回调 → putRepository() 或 deleteRepository()
  │
  ├─ 用户 PUT follow
  │     └─ RestoreService 按仓库名找 → 找到已注册的 CcrRepository ✓
  │
  └─ 分片分配阶段
        └─ CcrPrimaryFollowerAllocationDecider.canAllocate()
              → follower主分片 + 正在bootstrap + 节点无remote角色 → NO
              → 其他情况 → YES
```

---

## 四、哪些是 ES 框架已有的，哪些是 CCR 自己写的

### CCR 自己写的

| 组件 | 作用 |
|------|------|
| `CcrRepositoryManager` | 监听远程集群配置变化，自动注册/删除 CCR Repository |
| `RemoteSettingsUpdateListener` (内部类) | 继承 RemoteClusterAware，实现 updateRemoteCluster 回调 |
| `PutInternalCcrRepositoryAction` | 本地 Action，调用 registerInternalRepository |
| `DeleteInternalCcrRepositoryAction` | 本地 Action，调用 unregisterInternalRepository |
| `CcrPrimaryFollowerAllocationDecider` | 分片分配决策器，限制 bootstrap 阶段的分片去向 |

### ES 框架已有的

| 组件 | CCR 怎么用 |
|------|-----------|
| `RemoteClusterAware` | 继承，框架帮你监听远程集群配置变化 |
| `AbstractLifecycleComponent` | 继承，框架管理 doStart/doStop 生命周期 |
| `RepositoriesService.registerInternalRepository()` | 直接调用，注册内部仓库 |
| `AllocationDecider` | 继承，框架在分片分配时依次询问你 |
| `RemoteConnectionStrategy.isConnectionEnabled()` | 直接调用，判断连接是否启用 |
| `ClusterSettings.addAffixGroupUpdateConsumer()` | 框架内部调用，listenForUpdates 底层实现 |

### 注册位置（Ccr.java）

```java
// createComponents() 中创建 CcrRepositoryManager
new CcrRepositoryManager(settings, services.clusterService(), client)

// getActions() 中注册内部 Action
PutInternalCcrRepositoryAction.INSTANCE → TransportPutInternalRepositoryAction.class
DeleteInternalCcrRepositoryAction.INSTANCE → TransportDeleteInternalRepositoryAction.class

// createAllocationDeciders() 中返回决策器
return List.of(new CcrPrimaryFollowerAllocationDecider());
```

---

## 五、如果你自研类似功能

| 你需要做什么 | 对应 ES 框架扩展点 |
|---|---|
| 监听远程集群配置变化 | 继承 `RemoteClusterAware`，实现 `updateRemoteCluster()` |
| 注册内部 Repository | 调用 `RepositoriesService.registerInternalRepository()` |
| 管理组件生命周期 | 继承 `AbstractLifecycleComponent`，在 `createComponents()` 返回 |
| 限制分片分配到特定节点 | 继承 `AllocationDecider`，在 `createAllocationDeciders()` 返回 |

---

## 六、小结

1. **CcrRepositoryManager 是自动管家**——远程集群配多少，它就注册多少个假仓库。用户 PUT follow 时仓库已经就位。

2. **监听机制**用 ES 框架的 `RemoteClusterAware`，继承后实现一个回调方法即可。

3. **CcrPrimaryFollowerAllocationDecider 是分片分配看门人**——确保正在 bootstrap 的 follower 主分片只去有远程连接能力的节点。bootstrap 完成后不再限制。

4. **两者都是"提前准备基础设施"**，让后续的 PUT follow → restore → 拉数据流程能顺利执行。

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/CcrRepositoryManager.java` — Repository 自动管理
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/allocation/CcrPrimaryFollowerAllocationDecider.java` — 分片分配决策
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/repositories/PutInternalCcrRepositoryAction.java` — 注册仓库的内部 Action
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/Ccr.java` — 插件入口，注册以上组件
- `server/src/main/java/org/elasticsearch/transport/RemoteClusterAware.java` — ES 框架，远程集群配置监听基类
