# 第 7 课：插件入口与组件注册

## 一、核心问题

CCR 是一个 X-Pack 插件。ES 通过插件体系来扩展功能——你注册什么，ES 就给你启用什么。

这节课回答：**CCR 往 ES 里注册了哪些东西？它是怎么把自己"装进去"的？**

## 二、插件类声明

```java
public class Ccr extends Plugin
    implements ActionPlugin, PersistentTaskPlugin, EnginePlugin, RepositoryPlugin, ClusterPlugin {
```

| 接口 | 能注册什么 |
|------|-----------|
| `ActionPlugin` | Transport Action（集群内 RPC）和 REST Handler（HTTP API） |
| `PersistentTaskPlugin` | 持久化任务执行器 |
| `EnginePlugin` | 自定义 Lucene 引擎工厂 |
| `RepositoryPlugin` | 快照仓库实现 |
| `ClusterPlugin` | 分片分配决策器 |

## 三、什么是 Action

Action 是 ES 中**所有操作的统一抽象**——无论是用户 HTTP 请求，还是节点间内部通信，底层都是 Action。

**Action = ES 的 RPC 接口**，提供：
- 请求/响应的序列化（通过网络传输）
- 路由（知道发给哪个节点）
- 统一的调用模式：`client.execute(action, request, listener)`

### 两类 Action

| 类型 | 说明 | 调用方 |
|------|------|--------|
| 外部 Action | 有 REST Handler，用户通过 HTTP 调用 | 用户 |
| 内部 Action | 无 REST Handler，代码中调用，节点间 RPC | CCR 自己的代码 |

### 注册 Action = 注册路由

告诉 ES 的 Transport 层："收到这个名字的请求，用这个类来处理。" 不注册的话，Leader 节点收到 Follower 发来的请求就不知道交给谁处理。

### ES 不会主动调用 CCR 的 Action

调用链是：
1. **用户触发一次** `PUT /_ccr/follow` → 创建 persistent task
2. **Persistent task 启动后，CCR 代码自己循环调用**内部 Action（拉取、写入、续租）
3. ES 框架只提供执行环境，不主动驱动

```
用户: PUT /_ccr/follow
  → TransportPutFollowAction (外部 Action, 一次性)
    → 创建 PersistentTask
      → ShardFollowNodeTask 启动 (循环)
        ├── ShardChangesAction (Follower → Leader, 内部 Action)
        ├── BulkShardOperationsAction (Follower 本地, 内部 Action)
        └── RetentionLeaseActions.REMOTE_RENEW (Follower → Leader, 内部 Action)
```

## 四、注册了什么（按分类）

### 4.1 线程池

```java
new FixedExecutorBuilder(settings, "ccr", 32, 100, "xpack.ccr.ccr_thread_pool")
```

固定 32 线程，队列 100。CCR 所有后台工作跑在这个专用线程池。

### 4.2 Transport Actions

**数据传输类（核心复制链路）**

| Action | 方向 | 说明 |
|--------|------|------|
| `ShardChangesAction` | Follower → Leader | 拉取增量操作 |
| `BulkShardOperationsAction` | Follower 本地 | 将操作批量写入 Follower shard |
| `PutCcrRestoreSessionAction` | Follower → Leader | Bootstrap 时建立文件恢复会话 |
| `GetCcrRestoreFileChunkAction` | Follower → Leader | Bootstrap 时拉取 Lucene 文件分块 |
| `ClearCcrRestoreSessionAction` | Follower → Leader | 关闭文件恢复会话 |

**生命周期类（用户操作）**

| Action | API | 说明 |
|--------|-----|------|
| `PutFollowAction` | `PUT /<index>/_ccr/follow` | 创建 follower + 启动复制 |
| `ResumeFollowAction` | `POST /<index>/_ccr/resume_follow` | 恢复已暂停的复制 |
| `PauseFollowAction` | `POST /<index>/_ccr/pause_follow` | 暂停复制 |
| `UnfollowAction` | `POST /<index>/_ccr/unfollow` | 彻底解除 follow |
| `ForgetFollowerAction` | `POST /<index>/_ccr/forget_follower` | Leader 侧清理 retention lease |

**Auto-Follow 类**

| Action | 说明 |
|--------|------|
| `PutAutoFollowPatternAction` | 创建自动 follow 规则 |
| `DeleteAutoFollowPatternAction` | 删除规则 |
| `GetAutoFollowPatternAction` | 查询规则 |
| `ActivateAutoFollowPatternAction` | 激活/停用规则 |

### 4.3 PersistentTask 执行器

```java
new ShardFollowTasksExecutor(client, threadPool, clusterService, settingsModule)
```

每个 follower shard 的复制循环。

### 4.4 Engine 工厂

```java
if (CCR_FOLLOWING_INDEX_SETTING.get(indexSettings.getSettings())) {
    return Optional.of(new FollowingEngineFactory());  // follower 用 FollowingEngine
}
```

### 4.5 Repository

- 类型 `"_ccr_"`，内部仓库（用户不可见）
- Bootstrap 时伪装成 snapshot/restore，实际从 leader 实时拉取文件

### 4.6 AllocationDecider

`CcrPrimaryFollowerAllocationDecider`：follower primary shard 只能分配到有 `remote_cluster_client` 角色的节点。

### 4.7 后台组件

| 组件 | 说明 |
|------|------|
| `CcrRestoreSourceService` | Leader 侧：管理文件恢复会话 |
| `CcrRepositoryManager` | 跟随 remote cluster 配置自动创建/删除 Repository |
| `ShardFollowTaskCleaner` | follower 索引被删时自动清理 persistent task |
| `AutoFollowCoordinator` | 定期扫描 leader 新索引，自动 follow |

### 4.8 请求校验器

禁止用户手动修改 follower 索引的 mapping 和 aliases（只能从 leader 自动同步）。

## 五、CcrSettings——可调参数

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `xpack.ccr.enabled` | `true` | CCR 总开关 |
| `index.xpack.ccr.following_index` | `false` | 标记为 follower 索引（内部） |
| `ccr.indices.recovery.max_bytes_per_sec` | `40mb` | Bootstrap 限速 |
| `ccr.indices.recovery.chunk_size` | `1mb` | 文件块大小 |
| `ccr.indices.recovery.max_concurrent_file_chunks` | `5` | 并发文件块数 |
| `ccr.indices.recovery.recovery_activity_timeout` | `60s` | Leader 会话空闲超时 |
| `ccr.indices.recovery.internal_action_timeout` | `60s` | 内部 RPC 超时 |
| `ccr.wait_for_metadata_timeout` | `60s` | 等待 leader 元数据超时 |

## 六、小结

| 注册类别 | 内容 | 作用 |
|---------|------|------|
| 线程池 | "ccr"（32线程） | CCR 专用工作线程 |
| Transport Actions | ~20 个 | 数据传输 + 生命周期管理 + auto-follow |
| REST Handlers | ~13 个 | 暴露 HTTP API |
| PersistentTask | ShardFollowTasksExecutor | shard 级别复制循环 |
| Engine | FollowingEngineFactory | Follower 用特殊引擎 |
| Repository | CcrRepository | Bootstrap 用的内部快照仓库 |
| AllocationDecider | follower primary 必须能连 leader | 分片分配约束 |
| 后台组件 | 4 个 | 文件服务、仓库管理、任务清理、自动 follow |
| 请求校验 | mapping/aliases 写保护 | 禁止手动修改 follower |

**核心理解**：`Ccr.java` 是整个 CCR 功能的"接线板"，不包含业务逻辑，只负责把各个组件注册到 ES 框架的对应槽位中。Action 是 ES 插件与外界交互的唯一合法通道——注册 Action 就是注册路由，告诉 ES "收到这个请求用这个类处理"。ES 不会主动调用 CCR 的 Action，是 CCR 自己的 persistent task 在循环中驱动调用。

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/Ccr.java` — 插件入口，所有组件注册
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/CcrSettings.java` — 所有可调配置项
