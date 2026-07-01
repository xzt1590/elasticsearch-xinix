# 第 14 课：ShardFollowNodeTask（中）— 写入循环

## 一、核心问题

读循环把数据从 Leader 拉回来放进 buffer。写入循环的职责是：**从 buffer 取出数据，分批写入 Follower 本地分片。**

核心问题：
1. 什么时候触发写入？
2. 一次写多少？
3. 同时能发几个写请求？
4. 写完后做什么？

---

## 二、触发时机

写入不是定时触发的，有两个触发点：

1. **`innerHandleReadResponse()` 中**：读回来的数据放入 buffer 后，立即调 `coordinateWrites()`
2. **`handleWriteResponse()` 中**：上一批写完后，看 buffer 里还有没有剩余的，继续写

---

## 三、coordinateWrites() — 写入调度中心

```java
private synchronized void coordinateWrites() {
    if (isStopped()) {
        LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
        return;
    }

    while (hasWriteBudget() && buffer.isEmpty() == false) {
        long sumEstimatedSize = 0L;
        int length = Math.min(params.getMaxWriteRequestOperationCount(), buffer.size());
        List<Translog.Operation> ops = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            Translog.Operation op = buffer.remove();
            ops.add(op);
            sumEstimatedSize += op.estimateSize();
            if (sumEstimatedSize > params.getMaxWriteRequestSize().getBytes()) {
                break;
            }
        }
        bufferSizeInBytes -= sumEstimatedSize;
        numOutstandingWrites++;
        sendBulkShardOperationsRequest(ops, leaderMaxSeqNoOfUpdatesOrDeletes, new AtomicInteger(0));
    }
}
```

### 取数据逻辑

从 buffer 中取操作，受两个限制（取较严的那个）：
- **条数限制**：最多取 `maxWriteRequestOperationCount` 条（默认 5120）
- **字节限制**：累计字节超过 `maxWriteRequestSize`（默认 10MB）时提前 break

### while 循环的并发性

`sendBulkShardOperationsRequest` 内部的 `followerClient.execute()` 是**异步非阻塞**的——发出请求、注册回调、立即返回。所以 while 循环实际上是：

```
第1次：取一批 → execute(异步发出) → 立即返回 → numOutstandingWrites=1
第2次：取一批 → execute(异步发出) → 立即返回 → numOutstandingWrites=2
第3次：取一批 → execute(异步发出) → 立即返回 → numOutstandingWrites=3
...
第N次：hasWriteBudget()=false（到上限）→ 退出while
```

整个 while 执行时间极短（微秒级），多个写请求同时在飞、各自独立执行。

---

## 四、hasWriteBudget() — 写并发控制

```java
private boolean hasWriteBudget() {
    assert Thread.holdsLock(this);
    if (numOutstandingWrites >= params.getMaxOutstandingWriteRequests()) {
        return false;
    }
    return true;
}
```

只有一个限制：**并发写请求数不超过 `maxOutstandingWriteRequests`**（默认 9）。

比读简单——读有三道限制（并发数 + buffer 字节 + buffer 条数），写只有并发数这一道。因为写入时数据已经从 buffer 取出，不需要再限制 buffer。

---

## 五、sendBulkShardOperationsRequest — 发送写请求

```java
private void sendBulkShardOperationsRequest(
    List<Translog.Operation> operations,
    long leaderMaxSequenceNoOfUpdatesOrDeletes,
    AtomicInteger retryCounter) {
    
    final long startTime = relativeTimeProvider.getAsLong();
    innerSendBulkShardOperationsRequest(followerHistoryUUID, operations, leaderMaxSequenceNoOfUpdatesOrDeletes,
        response -> {
            synchronized (ShardFollowNodeTask.this) {
                totalWriteTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                successfulWriteRequests++;
                operationWritten += operations.size();
            }
            handleWriteResponse(response);
        },
        e -> {
            synchronized (ShardFollowNodeTask.this) {
                totalWriteTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                failedWriteRequests++;
            }
            handleFailure(e, retryCounter,
                () -> sendBulkShardOperationsRequest(operations, leaderMaxSequenceNoOfUpdatesOrDeletes, retryCounter));
        });
}
```

- 记录开始时间（统计用）
- 调用抽象方法 `innerSendBulkShardOperationsRequest`（匿名类提供的本地写入实现）
- 成功 → 更新统计 → `handleWriteResponse`
- 失败 → 更新统计 → `handleFailure`（可重试就带同一批 operations 重发，不可重试就致命失败）

**重试不会丢数据**：operations 列表已从 buffer 取出，保存在 lambda 闭包中。重试是拿同一批数据再写一次。

---

## 六、handleWriteResponse — 写完后的反馈

```java
private synchronized void handleWriteResponse(final BulkShardOperationsResponse response) {
    this.followerGlobalCheckpoint = Math.max(this.followerGlobalCheckpoint, response.getGlobalCheckpoint());
    this.followerMaxSeqNo = Math.max(this.followerMaxSeqNo, response.getMaxSeqNo());
    numOutstandingWrites--;
    assert numOutstandingWrites >= 0;
    coordinateWrites();
    coordinateReads();
}
```

四件事：
1. **更新 followerGlobalCheckpoint** — 写入成功后 Follower 的 GCP 推进了
2. **更新 followerMaxSeqNo**
3. **`coordinateWrites()`** — buffer 可能还有数据，继续写
4. **`coordinateReads()`** — buffer 减少了，读预算可能恢复，继续读

第 4 步是**反压恢复的关键**：之前 buffer 满导致读暂停，写完一批后 buffer 空出空间，`hasReadBudget()` 重新返回 true，读循环恢复。

---

## 七、反压机制完整图

```
coordinateReads()
  │ 发读请求到Leader
  ▼
数据回来 → buffer.addAll(operations)
  │          bufferSizeInBytes 增加
  │          hasReadBudget() 可能变false → 读暂停
  ▼
coordinateWrites()  ◄──────────────────┐
  │ 从buffer取出一批                     │
  │ bufferSizeInBytes 减少               │
  ▼                                     │
sendBulkShardOperationsRequest()        │
  │ 异步写入Follower本地                  │
  ▼                                     │
handleWriteResponse()                   │
  │ followerGCP 推进                     │
  │ numOutstandingWrites--               │
  ├─ coordinateWrites() ────────────────┘  (buffer还有就继续写)
  └─ coordinateReads()  → hasReadBudget()=true → 读恢复
```

---

## 八、leaderMaxSeqNoOfUpdatesOrDeletes 参数

写请求带了这个参数，它是 Leader 端所有 update/delete 操作的最大 seqNo。

Follower 的 FollowingEngine 用它做**乐观锁冲突检测**：判断某个 index 操作是否可能被后续的 update/delete 覆盖。如果一个 index 操作的 seqNo > leaderMaxSeqNoOfUpdatesOrDeletes，说明没有更晚的 update/delete 能覆盖它，可以安全写入。

细节在第 18 课（FollowingEngine）详讲。

---

## 九、和读循环对比

| | 读循环 | 写入循环 |
|---|---|---|
| 调度中心 | `coordinateReads()` | `coordinateWrites()` |
| 预算判断 | `hasReadBudget()`（三道限制） | `hasWriteBudget()`（一道限制） |
| 批次大小控制 | `maxReadRequestOperationCount` | `maxWriteRequestOperationCount` + `maxWriteRequestSize` |
| 并发上限 | `maxOutstandingReadRequests`（默认12） | `maxOutstandingWriteRequests`（默认9） |
| 数据方向 | Leader → buffer | buffer → Follower 本地 |
| 触发方式 | 自驱动（循环调用自己） | 被读触发 + 写完后自驱动 |
| 网络方向 | 跨集群 RPC（到 Leader） | 本地/集群内（到 Follower 主分片节点） |

---

## 十、小结

1. **写入由读触发**：读回数据放入 buffer → 调 coordinateWrites → 开始写。

2. **批次大小双重限制**：条数（5120）和字节（10MB），先到哪个上限就切一批。

3. **并发控制**：最多 9 个写请求同时在飞。while 循环中每次 execute 是异步的，所以多个写请求实际并发执行。

4. **反压机制**：写完后调 coordinateReads()，释放读预算，读写通过 buffer 自动协调。

5. **写失败不丢数据**：operations 保存在闭包中，重试用同一批数据重发。

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowNodeTask.java` — `coordinateWrites()`、`handleWriteResponse()`
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowTasksExecutor.java` — `innerSendBulkShardOperationsRequest()` 的具体实现
