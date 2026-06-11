# 第 5 课：Remote Cluster 连接机制

## 一、核心问题

CCR 是跨集群复制——follower 集群要从 leader 集群拉取数据。两个独立的 ES 集群之间怎么通信？

答案：**Remote Cluster 连接框架**——ES 原生提供的跨集群 TCP 长连接管理机制。

## 二、整体架构

```
RemoteClusterService                    ← 顶层管理器，维护所有远程集群连接
    └── Map<clusterAlias, RemoteClusterConnection>
                └── RemoteClusterConnection      ← 单个远程集群的连接封装
                        └── RemoteConnectionStrategy   ← 连接策略
                                ├── SniffConnectionStrategy   ← 探嗅模式（默认）
                                └── ProxyConnectionStrategy   ← 代理模式
```

CCR 调用链：
```
ShardFollowTasksExecutor / CcrRepository
    └── client.getRemoteClusterClient("leader_cluster", executor, strategy)
           └── 返回 RemoteClusterAwareClient（实现 RemoteClusterClient 接口）
                  └── execute(action, request, listener)
                         └── TransportService.sendRequest() → 通过 TCP 长连接发送到远端
```

对 CCR 代码完全透明——只需 `remoteClient.execute(action, request, listener)`，连接管理、重连、路由全由框架搞定。

## 三、两种连接模式

通过 `cluster.remote.<alias>.mode` 配置（默认 sniff）：

### 3.1 Sniff 模式（探嗅模式）

配置 seeds 节点，自动发现远端集群其他节点并建立连接：

```
cluster.remote.leader.seeds: ["10.0.0.1:9300", "10.0.0.2:9300"]
cluster.remote.leader.node_connections: 3   # 默认值
```

连接流程：连接 seed → 握手验证 → 请求远端节点列表 → 过滤后选 N 个建立持久连接。

每个连接 6 个 TCP 通道，3 节点 × 6 = 18 个总通道。

### 3.2 Proxy 模式（代理模式）

所有连接走固定代理地址，不做节点发现：

```
cluster.remote.leader.proxy_address: "lb.example.com:9300"
cluster.remote.leader.proxy_socket_connections: 18   # 默认值
```

每个连接 1 个通道，默认 18 个 socket。适合通过负载均衡器 / NAT / K8s Service 访问。

### 3.3 对比

| 维度 | Sniff（默认） | Proxy |
|------|--------------|-------|
| 配置 | seeds 节点列表 | 代理地址 |
| 节点发现 | 自动嗅探 | 不发现 |
| 连接拓扑 | 直连远端多个节点 | 所有流量走代理 |
| 适用场景 | 网络直通 | 通过 LB / NAT / K8s |

## 四、CCR 中的使用方式

### 4.1 获取客户端

```java
RemoteClusterClient remoteClient = client.getRemoteClusterClient(
    "leader_cluster",                                   // 远程集群别名
    executor,                                           // 响应处理线程池
    RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED  // 断连时自动重连
);
```

### 4.2 发送请求

```java
remoteClient.execute(
    RetentionLeaseActions.REMOTE_RENEW,    // 远程 Action 类型
    new RenewRequest(shardId, leaseId, seqNo, "ccr"),  // 请求参数
    listener                               // 异步回调
);
```

框架自动从连接池取一个 TCP 连接，发送 transport 请求到远端，远端路由到对应的 TransportAction 处理后返回响应。

## 五、连接维护机制

- **节点启动时**：自动对所有配置的远程集群建立连接（最多等 30 秒）
- **动态配置**：`cluster.remote.*` 支持 `PUT _cluster/settings` 热更新，无需重启
- **断连自动重连**：远端节点断开时，Sniff 模式会重新嗅探补充连接；Proxy 模式重连代理
- **ClusterName 验证**：握手时验证远端集群名，防止连错集群
- **连接合并**：多个并发的 `ensureConnected()` 只触发一次实际连接

## 六、远端扩缩容的影响

### Sniff 模式

- **远端节点下线**：自动检测断连 → 重新嗅探 → 连到新的可用节点
- **远端新增节点**：连接池满时不感知；有空缺时嗅探可能连到新节点
- **seed 全部替换**：当前连接不影响；重建连接时失败，需更新配置
- **远端滚动重启**：逐个断连+重连，基本无影响

### Proxy 模式

远端怎么变对本地完全透明，代理/LB 自己处理后端列表。

## 七、小结

| 概念 | 含义 |
|------|------|
| RemoteClusterService | 顶层管理器，提供 `getRemoteClusterClient()` |
| RemoteClusterClient | CCR 用来发请求的接口，`execute(action, request, listener)` |
| Sniff 模式 | 通过 seeds 自动发现节点，直连 |
| Proxy 模式 | 所有流量走固定代理 |
| 动态配置 | `cluster.remote.*` 可热更新 |
| 自动重连 | 断连后自动重新嗅探/重连 |

**核心理解**：Remote Cluster 是一个封装好的跨集群 TCP 连接池。CCR 代码不需要关心连接如何建立和维护，只需通过 `RemoteClusterClient` 接口发送请求即可。这是 CCR 跨集群通信的基础设施层。

---

## 涉及的源码文件

- `server/src/main/java/org/elasticsearch/transport/RemoteClusterService.java` — 顶层管理器
- `server/src/main/java/org/elasticsearch/transport/RemoteClusterConnection.java` — 单个远程集群连接封装
- `server/src/main/java/org/elasticsearch/transport/RemoteConnectionStrategy.java` — 连接策略抽象基类
- `server/src/main/java/org/elasticsearch/transport/SniffConnectionStrategy.java` — 探嗅模式实现
- `server/src/main/java/org/elasticsearch/transport/ProxyConnectionStrategy.java` — 代理模式实现
- `server/src/main/java/org/elasticsearch/client/internal/RemoteClusterClient.java` — 客户端接口
- `server/src/main/java/org/elasticsearch/transport/RemoteClusterAwareClient.java` — 客户端实现
