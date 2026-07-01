# 第 13 课：ShardFollowNodeTask（上）— 读取循环

## 一、核心问题

第 12 课的 `nodeOperation()` 最终调用了 `shardFollowNodeTask.start()`。那 start 之后具体发生了什么？数据是怎么从 Leader 持续流向 Follower 的？

本课聚焦**读取侧**：怎么调度读请求、一次读多少、同时能发几个、读回来的数据放哪。

---

## 二、关键状态变量

| 变量 | 含义 |
|------|------|
| `lastRequestedSeqNo` | "我已经请求到这个位置了"（不代表数据已回来） |
| `leaderGlobalCheckpoint` | Leader 当前确认到哪了（追赶目标） |
| `numOutstandingReads` | 当前有几个读请求在飞（发出去还没回来） |
| `buffer` | 读回来但还没写入 Follower 的操作，按 seqNo 排序的优先队列 |
| `bufferSizeInBytes` | buffer 当前占用字节数 |
| `partialReadRequests` | 上次没读完的范围，下次优先继续读 |
| `currentMappingVersion` | Follower 当前的 mapping 版本 |
| `currentSettingsVersion` | Follower 当前的 settings 版本 |
| `currentAliasesVersion` | Follower 当前的 aliases 版本 |

---

## 三、启动流程：start()

```java
void start(String followerHistoryUUID, long leaderGlobalCheckpoint, long leaderMaxSeqNo,
           long followerGlobalCheckpoint, long followerMaxSeqNo) {
    synchronized (this) {
        // 1. 初始化状态
        this.lastRequestedSeqNo = followerGlobalCheckpoint;  // 从Follower已确认位置开始
        this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
        // ...
        
        // 2. 启动租约续期定时器（独立运行）
        renewable = scheduleBackgroundRetentionLeaseRenewal(() -> this.followerGlobalCheckpoint);
    }

    // 3. 串行同步元数据：mapping → settings → aliases → 启动读循环
    updateMapping(0L, leaderMappingVersion -> {
        updateSettings(leaderSettingsVersion -> {
            updateAliases(leaderAliasesVersion -> {
                coordinateReads();   // ← 真正开始读
            });
        });
    });
}
```

执行顺序：
1. 初始化起点 `lastRequestedSeqNo = followerGlobalCheckpoint`
2. 启动租约续期定时器（后台独立运行，每30秒续期一次）
3. 无条件同步一次 mapping/settings/aliases（确保和 Leader 一致）
4. 三者完成后调用 `coordinateReads()` 进入读循环

---

## 四、核心：coordinateReads()

读循环的**调度中心**，决定"现在要不要发读请求、发几个、从哪开始"。

```java
synchronized void coordinateReads() {
    if (isStopped()) return;
    
    // 第一段：优先处理"上次没读完的范围"
    while (hasReadBudget() && partialReadRequests.isEmpty() == false) {
        Tuple<Long, Long> range = partialReadRequests.remove();
        numOutstandingReads++;
        sendShardChangesRequest(range.v1(), requestOpCount, range.v2());
    }
    
    // 第二段：发起新读请求，追赶 leaderGlobalCheckpoint
    while (hasReadBudget() && lastRequestedSeqNo < leaderGlobalCheckpoint) {
        long from = lastRequestedSeqNo + 1;
        long maxRequiredSeqNo = Math.min(leaderGlobalCheckpoint, from + maxReadRequestOperationCount - 1);
        numOutstandingReads++;
        lastRequestedSeqNo = maxRequiredSeqNo;
        sendShardChangesRequest(from, requestOpCount, maxRequiredSeqNo);
    }
    
    // 第三段：已追上，发探测请求（长轮询）
    if (numOutstandingReads == 0 && hasReadBudget()) {
        numOutstandingReads++;
        sendShardChangesRequest(lastRequestedSeqNo + 1, maxReadRequestOperationCount, lastRequestedSeqNo);
    }
}
```

### 三段逻辑

**第一段：处理 partialReadRequests**

如果上次请求"读 seqNo 100~1099"，Leader 只返回了 100~599（字节数到上限了），那 600~1099 就是"没读完的"，记入 `partialReadRequests`。下次优先续读。

**第二段：发起新读请求**

有预算且还没追上 Leader → 按 `maxReadRequestOperationCount` 分段发请求。每发一个就把 `lastRequestedSeqNo` 推进到该段终点。

优化：如果当前只有一个请求在飞，乐观地请求 `maxReadRequestOperationCount` 条（可能超出实际需要），因为不担心和其他请求重叠。

**第三段：探测（peek）**

已追上 Leader（`lastRequestedSeqNo == leaderGlobalCheckpoint`）。发一个探测请求，Leader 没新数据时长轮询等待，有新数据再返回。实现"追上后低延迟感知新数据"。

---

## 五、hasReadBudget() — 三道限制

```java
private boolean hasReadBudget() {
    if (numOutstandingReads >= params.getMaxOutstandingReadRequests()) {
        return false;  // 并发读到上限（默认12）
    }
    if (bufferSizeInBytes >= params.getMaxWriteBufferSize().getBytes()) {
        return false;  // 缓冲区字节到上限（默认10MB）
    }
    if (buffer.size() >= params.getMaxWriteBufferCount()) {
        return false;  // 缓冲区条数到上限（默认10240）
    }
    return true;
}
```

三个条件任一不满足就停止读。这是**反压机制**：写入跟不上读取时，buffer 堆积，读自动暂停。写入消化 buffer 后，读自动恢复。

---

## 六、sendShardChangesRequest — 发送读请求

```java
private void sendShardChangesRequest(long from, int maxOperationCount, long maxRequiredSeqNo, AtomicInteger retryCounter) {
    final long startTime = relativeTimeProvider.getAsLong();
    innerSendShardChangesRequest(from, maxOperationCount, response -> {
        // 成功：更新统计 → handleReadResponse
        synchronized (ShardFollowNodeTask.this) {
            fetchExceptions.remove(from);
            totalReadRemoteExecTimeMillis += response.getTookInMillis();
            successfulReadRequests++;
            operationsRead += response.getOperations().length;
        }
        handleReadResponse(from, maxRequiredSeqNo, response);
    }, e -> {
        // 失败：更新统计 → 判断是否可重试
        synchronized (ShardFollowNodeTask.this) {
            failedReadRequests++;
            fetchExceptions.put(from, Tuple.tuple(retryCounter, ...));
        }
        if (是 REQUESTED_OPS_MISSING) {
            handleFallenBehindLeaderShard(...);  // 落后太多
        } else {
            handleFailure(e, retryCounter, () -> sendShardChangesRequest(...));  // 普通重试
        }
    });
}
```

- 调用抽象方法 `innerSendShardChangesRequest`（第12课匿名类提供的 RPC 实现）
- 成功 → 更新统计 → `handleReadResponse`
- 失败 → 更新统计 → 重试或致命失败
- 特殊情况：Leader 返回"请求的操作已不存在"（被 merge 清理了）→ `handleFallenBehindLeaderShard`

---

## 七、handleReadResponse — 读到数据后的处理链

```java
void handleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
    Runnable handleResponseTask = () -> innerHandleReadResponse(from, maxRequiredSeqNo, response);
    Runnable updateMappingsTask = () -> maybeUpdateMapping(response.getMappingVersion(), handleResponseTask);
    Runnable updateSettingsTask = () -> maybeUpdateSettings(response.getSettingsVersion(), updateMappingsTask);
    maybeUpdateAliases(response.getAliasesVersion(), updateSettingsTask);
}
```

实际执行顺序（从外到内）：
1. 检查 aliases 版本 → 落后则同步
2. 检查 settings 版本 → 落后则同步
3. 检查 mapping 版本 → 落后则同步
4. 元数据到位后，处理读取的数据

`maybeUpdateXxx` 内部比较版本号：本地够了就直接 `task.run()` 跳过，不够就触发同步。**大多数时候三个都跳过**，直接进入 `innerHandleReadResponse`。

---

## 八、innerHandleReadResponse — 数据进入 buffer

```java
synchronized void innerHandleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
    // 1. 更新对 Leader 的认知
    leaderGlobalCheckpoint = Math.max(leaderGlobalCheckpoint, response.getGlobalCheckpoint());
    leaderMaxSeqNo = Math.max(leaderMaxSeqNo, response.getMaxSeqNo());
    
    if (response.getOperations().length == 0) {
        newFromSeqNo = from;  // 长轮询返回空，不处理
    } else {
        // 2. 数据放入 buffer
        buffer.addAll(operations);
        bufferSizeInBytes += operationsSize;
        
        // 3. 更新 lastRequestedSeqNo
        lastRequestedSeqNo = Math.max(lastRequestedSeqNo, maxSeqNo);
        
        // 4. 触发写入
        coordinateWrites();
        
        newFromSeqNo = maxSeqNo + 1;
    }
    
    // 5. 没读完？记录为 partial，下次优先续读
    if (newFromSeqNo <= maxRequiredSeqNo) {
        partialReadRequests.add(Tuple.tuple(newFromSeqNo, maxRequiredSeqNo));
    }
    
    // 6. 释放并发名额，继续调度
    numOutstandingReads--;
    coordinateReads();
}
```

关键动作：
1. 更新追赶目标（Leader 的 GCP 可能又往前推了）
2. 数据放入 buffer
3. 触发 `coordinateWrites()`（有数据了，写入侧开始工作）
4. 没读完的记为 partial
5. 释放名额，再次调 `coordinateReads()`

最后一步形成循环：`coordinateReads → send → innerHandleReadResponse → coordinateReads → ...`

**注意**：这不是同步递归。中间有异步网络 IO 作为边界，每次回调都是全新的栈帧，不会栈溢出。

---

## 九、反压机制

读和写通过 buffer 耦合：

```
读太快、写太慢 → buffer 堆积 → hasReadBudget()=false → 读暂停
写完一批 → buffer 减少 → handleWriteResponse() 调 coordinateReads() → 读恢复
```

保证不会无限制把内存撑爆。

---

## 十、整体读循环流程图

```
start()
  │  ① 启动租约续期（后台独立运行）
  │  ② 初始同步 mapping/settings/aliases
  ▼
coordinateReads()  ◄──────────────────────────────────────┐
  │                                                        │
  ├─ hasReadBudget()? No → 等待（写入消化buffer后回来）       │
  │                                                        │
  ├─ 有 partialReadRequests → 优先续读                       │
  ├─ lastRequestedSeqNo < leaderGCP → 发新请求               │
  ├─ 已追上 → 发探测请求（长轮询）                             │
  │                                                        │
  ▼                                                        │
sendShardChangesRequest() → RPC到Leader                     │
  │                                                        │
  ├─ 失败 → handleFailure → 延迟重试                         │
  ▼ 成功                                                    │
handleReadResponse()                                       │
  │  maybeUpdate Aliases/Settings/Mapping（按需）             │
  ▼                                                        │
innerHandleReadResponse()                                  │
  │  数据放入buffer → coordinateWrites()                     │
  │  numOutstandingReads--                                  │
  └─ coordinateReads() ────────────────────────────────────┘
```

---

## 十一、6 个抽象方法的执行顺序

```
start()
  ├─ ① scheduleBackgroundRetentionLeaseRenewal（启动后独立运行，每30秒一次）
  ├─ ② innerUpdateMapping（初始同步，无条件）
  ├─ ③ innerUpdateSettings（初始同步）
  ├─ ④ innerUpdateAliases（初始同步）
  ▼ 进入循环
  ├─ ⑤ innerSendShardChangesRequest（读操作数据）
  ├─ [按需] ②③④ 再次执行（对比版本号，落后才触发）
  ├─ ⑥ innerSendBulkShardOperationsRequest（写入Follower）
  └─ 回到 ⑤ ...
```

---

## 十二、哪些是 ES 框架，哪些是 CCR 自己写的

### CCR 自己写的

| 组件 | 作用 |
|------|------|
| `ShardFollowNodeTask` 整个类 | 读写循环状态机 |
| `coordinateReads()` | 读调度中心 |
| `hasReadBudget()` | 反压判断 |
| `handleReadResponse()` / `innerHandleReadResponse()` | 处理读响应 |
| buffer + partialReadRequests | 数据缓冲与未完成请求管理 |

### ES 框架已有的

| 组件 | CCR 怎么用 |
|------|-----------|
| `AllocatedPersistentTask` | ShardFollowNodeTask 继承它，获得任务生命周期管理 |
| `Scheduler.Cancellable` / `scheduleWithFixedDelay` | 租约续期定时器 |
| `PriorityQueue` | Java 标准库，用于 buffer 和 partialReadRequests 的排序 |

---

## 涉及的源码文件

- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowNodeTask.java` — 本课主角
- `x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowTasksExecutor.java` — 提供6个抽象方法的具体实现
