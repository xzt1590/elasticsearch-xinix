# 第 15 课：ShardFollowNodeTask（下）— 异常处理与恢复

## 一、核心问题

读写循环中任何一步都可能失败。本课回答：
1. 哪些错误可以重试？哪些是致命的？
2. 重试的策略是什么？等多久？
3. "落后太多"这种特殊情况怎么处理？

---

## 二、shouldRetry() — 判断是否可重试

```java
static boolean shouldRetry(final Exception e) {
    if (NetworkExceptionHelper.isConnectException(e)
        || NetworkExceptionHelper.getCloseConnectionExceptionLevel(e, false) != Level.OFF) {
        return true;
    }

    final Throwable actual = ExceptionsHelper.unwrapCause(e);
    return actual instanceof ShardNotFoundException
        || actual instanceof IllegalIndexShardStateException
        || actual instanceof NoShardAvailableActionException
        || actual instanceof UnavailableShardsException
        || actual instanceof AlreadyClosedException
        || actual instanceof ElasticsearchSecurityException
        || actual instanceof ClusterBlockException
        || actual instanceof IndexClosedException
        || actual instanceof ConnectTransportException
        || actual instanceof NodeClosedException
        || actual instanceof NoSuchRemoteClusterException
        || actual instanceof NoSeedNodeLeftException
        || actual instanceof EsRejectedExecutionException
        || actual instanceof CircuitBreakingException;
}
```

### 可重试的（暂时性错误）

| 错误类型 | 典型场景 |
|---------|---------|
| 网络连接异常 | Leader 暂时不可达 |
| ShardNotFoundException | 分片正在迁移 |
| IllegalIndexShardStateException | 分片正在恢复中 |
| NoShardAvailableActionException | 分片暂时没有可用副本 |
| AlreadyClosedException | 分片正在关闭（relocate） |
| ElasticsearchSecurityException | 权限暂时不可用（证书轮换中） |
| ClusterBlockException | Leader 索引被临时 block 或无 master |
| IndexClosedException | Follower 索引被关闭（updateSettings 期间） |
| ConnectTransportException | 传输层连接失败 |
| NodeClosedException | 目标节点正在重启 |
| NoSuchRemoteClusterException | 远程集群配置暂时不可用 |
| EsRejectedExecutionException | 线程池满了 |
| CircuitBreakingException | 内存熔断 |

### 不可重试的（致命错误）

所有不在上面列表中的异常。比如 `IndexNotFoundException`（索引被删了）、`MapperParsingException`（数据和 mapping 不兼容）等。这些不会因为重试就好转。

---

## 三、handleFailure() — 统一错误处理

```java
private void handleFailure(Exception e, AtomicInteger retryCounter, Runnable task) {
    assert e != null;
    if (shouldRetry(e)) {
        if (isStopped() == false) {
            int currentRetry = retryCounter.incrementAndGet();
            LOGGER.debug(() -> format("%s error during follow shard task, retrying [%s]",
                params.getFollowShardId(), currentRetry), e);
            long delay = computeDelay(currentRetry, params.getReadPollTimeout().getMillis());
            scheduler.accept(TimeValue.timeValueMillis(delay), task);
        }
    } else {
        onFatalFailure(e);
    }
}
```

逻辑：
1. 判断是否可重试
2. 可重试 → 检查任务没停 → retryCounter++ → 计算延迟 → 延迟后重新执行 task
3. 不可重试 → `onFatalFailure`

`task` 就是"重新执行失败的那个操作"：
- 读失败：`() -> sendShardChangesRequest(from, ...)`
- 写失败：`() -> sendBulkShardOperationsRequest(operations, ...)`
- 同步 mapping 失败：`() -> updateMapping(version, handler, retryCounter)`

---

## 四、computeDelay() — 指数退避 + 随机抖动

```java
static long computeDelay(int currentRetry, long maxRetryDelayInMillis) {
    int maxCurrentRetry = Math.min(currentRetry, 24);
    long n = Math.round(Math.pow(2, maxCurrentRetry - 1));
    int k = Randomness.get().nextInt(Math.toIntExact(n + 1));
    int backOffDelay = k * DELAY_MILLIS;
    return Math.min(backOffDelay, maxRetryDelayInMillis);
}
```

- `DELAY_MILLIS = 50` — 基础单位 50ms
- `n = 2^(retry-1)` — 指数增长
- `k = random(0, n)` — 随机抖动（jitter）
- `delay = k * 50ms`，上限为 `maxRetryDelayInMillis`（通常 1 分钟）
- `maxCurrentRetry` 最大 24 防溢出

### 举例

| 重试次数 | n | 可能的延迟范围 |
|---------|---|--------------|
| 1 | 1 | 0~50ms |
| 2 | 2 | 0~100ms |
| 5 | 16 | 0~800ms |
| 10 | 512 | 0~25.6s |
| 15+ | 16384+ | 撞上限，固定 60s |

### 为什么加随机

防止"惊群效应"：多个 Follower 分片同时失败时，没有 jitter 会在同一时刻一起重试，压垮 Leader。加了随机后重试分散开。

---

## 五、onFatalFailure() — 致命失败处理

```java
final void onFatalFailure(Exception e) {
    synchronized (this) {
        this.fatalException = ExceptionsHelper.convertToElastic(e);
        if (this.renewable != null) {
            this.renewable.cancel();
            this.renewable = null;
        }
    }
    LOGGER.warn("shard follow task encounter non-retryable error", e);
}
```

做三件事：
1. 记录异常到 `fatalException`（通过 `getStatus()` 暴露给用户）
2. 取消租约续期定时器
3. 打 WARN 日志

之后 `isStopped()` 返回 true，所有循环入口检查后直接 return，整个任务静默停止。

**注意**：不会删除持久化任务。任务仍存在于集群状态中，状态变为 failed。用户通过 `GET /_ccr/stats` 看到失败原因，手动 resume 或 unfollow。

---

## 六、handleFallenBehindLeaderShard — 落后太多

```java
void handleFallenBehindLeaderShard(Exception e, long from, int maxOperationCount,
    long maxRequiredSeqNo, AtomicInteger retryCounter) {
    handleFailure(e, retryCounter, () -> sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, retryCounter));
}
```

### 触发条件

Leader 返回 `ResourceNotFoundException` 且带有 `REQUESTED_OPS_MISSING_METADATA_KEY`。意思是：Follower 请求的 seqNo 范围的操作已被 Lucene merge 清理掉了。

### 为什么会发生

- 租约续期失败了（网络长时间中断）
- 或者租约过期（Follower 离线超过 12 小时）
- Leader 不再为 Follower 保留那些历史操作

### 正确处理方式

理论上应该**重新 bootstrap**（重走 snapshot/restore 流程）。但从代码注释看，目前暂时走重试逻辑（会反复失败直到人工介入），未来会改为自动触发重新 restore。

---

## 七、isStopped() 与 onCancelled()

### 停止条件

```java
protected boolean isStopped() {
    return fatalException != null || isCancelled() || isCompleted();
}
```

三种情况任一为 true 就停：
- `fatalException != null` — 遇到不可重试错误
- `isCancelled()` — 用户执行了 pause 或 unfollow
- `isCompleted()` — 任务正常完成

### 用户主动取消

```java
@Override
protected void onCancelled() {
    synchronized (this) {
        if (renewable != null) {
            renewable.cancel();
            renewable = null;
        }
    }
    markAsCompleted();
}
```

用户 pause/unfollow → 框架调用 `onCancelled()` → 取消租约定时器 → 标记完成 → `isStopped()=true` → 所有循环停止。

---

## 八、错误处理整体流程图

```
任何操作失败（读/写/同步元数据）
  │
  ▼
handleFailure(e, retryCounter, task)
  │
  ├─ shouldRetry(e) = true
  │     │
  │     ├─ 任务未停
  │     │     retryCounter++
  │     │     delay = computeDelay(retryCounter)  [指数退避+抖动]
  │     │     scheduler.accept(delay, task)        [延迟后重试]
  │     │
  │     └─ 任务已停 → 忽略
  │
  └─ shouldRetry(e) = false
        │
        ▼
      onFatalFailure(e)
        │  记录 fatalException
        │  取消租约定时器
        │  isStopped() = true
        ▼
      所有循环静默停止
      用户通过 GET /_ccr/stats 看到失败原因
      手动 resume 恢复 或 unfollow 清理
```

---

## 九、小结

1. **可重试 vs 致命**：暂时性问题（网络、分片迁移、熔断等）可重试；永久性问题（索引被删、mapping 不兼容等）致命停止。

2. **退避策略**：指数退避 + 随机抖动，从 50ms 逐渐增长到上限（默认 60s），防止惊群效应。

3. **致命失败不删任务**：记录异常，停止循环，等用户介入。

4. **落后太多**：租约失效后历史被清理，目前走重试（会失败），未来改为自动重新 bootstrap。

5. **优雅停止**：`onCancelled()` 取消定时器 + 标记完成，所有循环通过 `isStopped()` 检查后静默退出。

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowNodeTask.java` — `handleFailure()`、`shouldRetry()`、`computeDelay()`、`onFatalFailure()`、`handleFallenBehindLeaderShard()`
