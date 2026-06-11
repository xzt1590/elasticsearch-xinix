# CCR 精读课程大纲

> 目标：从前置知识到核心实现，逐步读懂 Elasticsearch Cross-Cluster Replication 功能。

---

## 第一单元：前置知识 — 理解 CCR 的地基

每节课聚焦一个核心概念，目标是"能用自己的话解释清楚"。

---

### 第 1 课：Sequence Number 与 Checkpoint

- 什么是 seq_no，为什么每个写操作需要一个全局递增编号
- Local Checkpoint vs Global Checkpoint 的含义与推进机制
- 为什么 CCR 需要这套机制（追踪"我复制到哪了"）
- 代码：
  - `server/src/main/java/org/elasticsearch/index/seqno/SequenceNumbers.java`
  - `server/src/main/java/org/elasticsearch/index/seqno/LocalCheckpointTracker.java`
  - `server/src/main/java/org/elasticsearch/index/seqno/GlobalCheckpointTracker.java`

### 第 2 课：Soft Deletes 与历史操作保留

- Lucene 删除文档的两种方式：硬删除 vs 软删除
- 软删除如何让 ES 保留"已删除/已覆盖"的操作历史
- `LuceneChangesSnapshot`：如何从 Lucene 中按 seq_no 范围读取历史操作
- 为什么没有 Soft Deletes 就没有 CCR
- 代码：
  - `server/src/main/java/org/elasticsearch/index/engine/LuceneChangesSnapshot.java`

### 第 3 课：Retention Lease（保留租约）

- 问题：软删除的数据什么时候会被 Lucene merge 掉？
- Retention Lease 的作用：告诉引擎"这个 seq_no 之后的别删"
- 租约的续期与过期机制
- 代码：
  - `server/src/main/java/org/elasticsearch/index/seqno/RetentionLease.java`
  - `server/src/main/java/org/elasticsearch/index/seqno/RetentionLeases.java`

### 第 4 课：PersistentTasks 框架

- 什么是持久化任务（跨节点重启存活的后台任务）
- 任务的生命周期：创建 → 分配 → 执行 → 完成/失败
- 节点宕机后任务如何重新分配
- 代码：
  - `server/src/main/java/org/elasticsearch/persistent/PersistentTasksExecutor.java`
  - `server/src/main/java/org/elasticsearch/persistent/AllocatedPersistentTask.java`

### 第 5 课：Remote Cluster 连接机制

- ES 集群之间如何建立连接
- Sniff 模式 vs Proxy 模式
- `RemoteClusterService` 如何管理多个远程集群连接
- 安全性：跨集群请求的认证与权限传递
- 代码：
  - `server/src/main/java/org/elasticsearch/transport/RemoteClusterService.java`
  - `server/src/main/java/org/elasticsearch/transport/RemoteClusterConnection.java`

---

## 第二单元：CCR 全景 — 从外部视角看功能

先建立宏观理解，再深入细节。

---

### 第 6 课：CCR 是什么、解决什么问题

- 使用场景：灾备、就近读取、集中报表
- CCR vs 主副本复制的本质区别
- Leader/Follower 模型概述
- 动手：用 REST API 操作一遍完整的 follow 流程

### 第 7 课：插件入口与组件注册

- `Ccr.java` 逐段解读
- CCR 注册了哪些组件：Actions、REST handlers、Engine、Repository、PersistentTask
- CCR 专用线程池的配置
- `CcrSettings.java`：所有可调参数一览
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/Ccr.java`
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/CcrSettings.java`

---

## 第三单元：初始引导 — Follower 索引从无到有

---

### 第 8 课：PUT follow 的完整流程

- 用户调用 `PUT /{index}/_ccr/follow` 之后发生了什么
- `RestPutFollowAction` → `TransportPutFollowAction` 的调用链
- 三步走：验证 → 创建索引(restore) → 启动复制任务
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/rest/RestPutFollowAction.java`
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/TransportPutFollowAction.java`

### 第 9 课：CcrRepository — 基于 Snapshot/Restore 的数据引导

- 为什么用 snapshot/restore 而不是重放全部 translog
- `CcrRepository` 如何伪装成一个 Repository
- Leader 端 `CcrRestoreSourceService`：文件会话管理
- 文件传输过程：打开会话 → 分块传输 → 关闭会话
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/repository/CcrRepository.java`
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/repository/CcrRestoreSourceService.java`

### 第 10 课：CcrRepositoryManager 与分片分配

- 每个远程集群对应一个内部 CCR Repository
- `CcrRepositoryManager` 如何监听远程集群配置变化
- `CcrPrimaryFollowerAllocationDecider`：为什么 follower 主分片必须在有 remote_cluster_client 角色的节点上
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/CcrRepositoryManager.java`
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/allocation/CcrPrimaryFollowerAllocationDecider.java`

---

## 第四单元：持续复制循环 — CCR 的心脏

这是最核心的部分，会拆得最细。

---

### 第 11 课：ShardFollowTasksExecutor — 任务启动与元数据同步

- 如何为每个分片创建一个 follow task
- Mapping/Settings/Aliases 的同步机制
- 任务失败后的重试策略
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowTasksExecutor.java`

### 第 12 课：ShardFollowNodeTask（上）— 读取循环

- `coordinateReads()`：核心状态机
- 并发读请求的控制（max concurrent reads）
- 读请求大小的控制（max read request size / operation count）
- 长轮询机制：没有新数据时怎么等待
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowNodeTask.java`
  - 重点方法：`coordinateReads()`、`sendShardChangesRequest()`

### 第 13 课：ShardFollowNodeTask（中）— 写入循环

- `coordinateWrites()`：缓冲区管理
- 并发写请求的控制
- 写入缓冲区的水位控制（buffer limit）
- 读写之间的协调：反压机制
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowNodeTask.java`
  - 重点方法：`coordinateWrites()`、`sendBulkShardOperationsRequest()`

### 第 14 课：ShardFollowNodeTask（下）— 异常处理与恢复

- 可重试异常 vs 致命异常
- 读失败时的退避重试
- 写失败时的处理策略
- Shard 级别的 fatal error 如何上报
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowNodeTask.java`
  - 重点方法：`handleReadResponse()`、`handleFailure()`

### 第 15 课：ShardChangesAction — Leader 端如何提供数据

- 请求参数：from_seq_no、max_operations、poll_timeout
- 底层调用 `LuceneChangesSnapshot` 读取操作
- Global Checkpoint 的长轮询等待实现
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardChangesAction.java`

### 第 16 课：BulkShardOperationsAction — Follower 端如何写入

- 批量写入的事务语义
- 如何处理操作冲突（乐观锁）
- 与 FollowingEngine 的交互
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/bulk/TransportBulkShardOperationsAction.java`

---

## 第五单元：FollowingEngine — 特殊引擎

---

### 第 17 课：FollowingEngine 的设计

- 为什么 Follower 不能用标准 InternalEngine
- 关键差异：使用 Leader 的 seq_no 而非自己分配
- 如何处理重复操作（幂等性保证）
- `planIndexingAsPrimary` 的行为变化
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/index/engine/FollowingEngine.java`
  - 对比阅读：`server/src/main/java/org/elasticsearch/index/engine/InternalEngine.java`

---

## 第六单元：Auto-Follow — 自动化

---

### 第 18 课：AutoFollowCoordinator

- 只在 master 节点运行的协调器
- 如何定期扫描远程集群的索引列表
- Pattern 匹配与排除逻辑
- 发现新索引后如何自动发起 follow
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/AutoFollowCoordinator.java`

### 第 19 课：AutoFollowMetadata

- 集群状态中的 auto-follow 元数据结构
- 已 follow 索引的记录与去重
- Pause/Resume auto-follow pattern 的实现
- 代码：
  - `x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/ccr/AutoFollowMetadata.java`

---

## 第七单元：辅助机制与运维

---

### 第 20 课：CcrRetentionLeases — 保证数据不丢

- Follower 如何在 Leader 上维护保留租约
- 续期频率与过期时间
- 租约丢失后会发生什么（需要重新 bootstrap）
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/CcrRetentionLeases.java`

### 第 21 课：Pause / Resume / Unfollow 生命周期

- Pause：取消持久化任务但保留 follower 索引状态
- Resume：重新创建任务，从上次的 global checkpoint 继续
- Unfollow：彻底清理，将 follower 转为普通索引
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/TransportPauseFollowAction.java`
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/TransportResumeFollowAction.java`
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/TransportUnfollowAction.java`

### 第 22 课：监控与故障排查

- `GET /{index}/_ccr/stats` 返回的指标含义
- 常见异常及处理：leader_global_checkpoint 落后、retention lease 过期、mapping 不兼容
- 许可证门控：`CcrLicenseChecker` 的双端验证
- 代码：
  - `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/CcrLicenseChecker.java`

---

## 第八单元：通过测试巩固理解

---

### 第 23 课：ShardFollowNodeTaskTests 精读

- 测试如何 mock Leader 端行为
- 典型场景：正常复制、读超时、写失败、缓冲区满
- 通过测试用例验证你对复制循环的理解
- 代码：
  - `x-pack/plugin/ccr/src/test/java/org/elasticsearch/xpack/ccr/action/ShardFollowNodeTaskTests.java`

### 第 24 课：IndexFollowingIT 集成测试

- 端到端流程的测试组织方式
- 如何在测试中模拟双集群环境
- 学习写 CCR 相关测试的方法
- 代码：
  - `x-pack/plugin/ccr/src/internalClusterTest/java/org/elasticsearch/xpack/ccr/IndexFollowingIT.java`

---

## 课程进度追踪

在下方标记你的学习进度：

- [✅] 第 1 课：Sequence Number 与 Checkpoint
- [✅] 第 2 课：Soft Deletes 与历史操作保留
- [✅] 第 3 课：Retention Lease（保留租约）
- [✅] 第 4 课：PersistentTasks 框架
- [ ] 第 5 课：Remote Cluster 连接机制
- [ ] 第 6 课：CCR 是什么、解决什么问题
- [ ] 第 7 课：插件入口与组件注册
- [ ] 第 8 课：PUT follow 的完整流程
- [ ] 第 9 课：CcrRepository — 基于 Snapshot/Restore 的数据引导
- [ ] 第 10 课：CcrRepositoryManager 与分片分配
- [ ] 第 11 课：ShardFollowTasksExecutor — 任务启动与元数据同步
- [ ] 第 12 课：ShardFollowNodeTask（上）— 读取循环
- [ ] 第 13 课：ShardFollowNodeTask（中）— 写入循环
- [ ] 第 14 课：ShardFollowNodeTask（下）— 异常处理与恢复
- [ ] 第 15 课：ShardChangesAction — Leader 端如何提供数据
- [ ] 第 16 课：BulkShardOperationsAction — Follower 端如何写入
- [ ] 第 17 课：FollowingEngine 的设计
- [ ] 第 18 课：AutoFollowCoordinator
- [ ] 第 19 课：AutoFollowMetadata
- [ ] 第 20 课：CcrRetentionLeases — 保证数据不丢
- [ ] 第 21 课：Pause / Resume / Unfollow 生命周期
- [ ] 第 22 课：监控与故障排查
- [ ] 第 23 课：ShardFollowNodeTaskTests 精读
- [ ] 第 24 课：IndexFollowingIT 集成测试

---

## 使用方式

在新的 session 中，告诉 AI：

> 请阅读 `CCR_LEARNING_GUIDE.md`，我们继续从第 X 课开始学习。

AI 会根据大纲中指定的代码文件，带你逐步精读源码。
