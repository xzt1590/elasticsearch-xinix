# 第 1 课：Sequence Number 与 Checkpoint

## 一、为什么需要 Sequence Number？

一个索引有 1 个 primary shard 和若干 replica shard。当用户写入文档时，primary 处理完后要把操作复制到 replica。

核心问题：**怎么知道 replica 复制到哪了？怎么知道哪些操作是新的？**

解决方案：给每个写操作编一个严格递增的编号——Sequence Number。

## 二、核心概念

### 2.1 Sequence Number (seq_no)

每个写操作（index、delete、update）在 primary shard 上执行时，被分配一个严格递增的序列号。

代码位置：`server/src/main/java/org/elasticsearch/index/seqno/LocalCheckpointTracker.java:84`

```java
public long generateSeqNo() {
    return nextSeqNo.getAndIncrement();
}
```

关键常量（`SequenceNumbers.java`）：

```java
public static final long UNASSIGNED_SEQ_NO = -2L;   // 操作还没分配序号（如 primary 执行前）
public static final long NO_OPS_PERFORMED = -1L;     // 这个 shard 还没执行过任何操作
```

### 2.2 Local Checkpoint（本地检查点）

**定义**：该分片上，所有 seq_no <= local_checkpoint 的操作都已处理完毕。

关键点：local checkpoint 是一个**连续完成的最高位**，不是最大已完成的 seq_no。

举例：

```
操作到达顺序:  seq_no = 0, 1, 3, 2, 4
完成状态:      ✓  ✓  ✓  ✓  ✓

当 0,1 完成时:    local_checkpoint = 1
当 3 完成时:      local_checkpoint = 1  （因为 2 还没完成，不连续！）
当 2 完成时:      local_checkpoint = 3  （0,1,2,3 都连续了，一下推进到 3）
当 4 完成时:      local_checkpoint = 4
```

### 2.3 Global Checkpoint（全局检查点）

**定义**：所有 in-sync 副本的 local checkpoint 的最小值。

```
Global Checkpoint = min(所有 in-sync 副本的 local checkpoint)
```

- **谁来计算？** Primary shard
- **谁来维护？** `ReplicationTracker`（合并了以前的 `GlobalCheckpointTracker`）

代码位置：`server/src/main/java/org/elasticsearch/index/seqno/ReplicationTracker.java:68`

> The global checkpoint is the highest sequence number for which all lower (or equal) sequence number have been processed on all shards that are currently active.

### 2.4 Primary Term

代码位置：`ReplicationTracker.java:99`

```java
private volatile long operationPrimaryTerm;
```

Primary Term 是一个代际编号，每次 primary shard 切换（故障转移）时 +1。它和 seq_no 组合在一起唯一确定一个操作的"身份"——即使 seq_no 相同，不同 primary term 下的操作也是不同的。

## 三、LocalCheckpointTracker 的数据结构详解

### 3.1 为什么不用一个大 BitSet？

1. seq_no 可能涨到几十亿，不能预分配
2. 已推进过 checkpoint 的部分永远不需要再查，应该释放内存

解决方案：把逻辑上的大 BitSet **分成多段**，每段 1024 位，用完就丢。

### 3.2 分段 BitSet 结构

```
BIT_SET_SIZE = 1024

bitSetMap = Map<Long, CountedBitSet>

key=0 → [bit0, bit1, ..., bit1023]     覆盖 seq_no 0~1023
key=1 → [bit0, bit1, ..., bit1023]     覆盖 seq_no 1024~2047
key=2 → [bit0, bit1, ..., bit1023]     覆盖 seq_no 2048~3071
...
```

key 的计算：

```java
private static long getBitSetKey(final long seqNo) {
    return seqNo / BIT_SET_SIZE;   // 整除 1024
}
```

偏移量的计算：

```java
private static int seqNoToBitSetOffset(final long seqNo) {
    return Math.toIntExact(seqNo % BIT_SET_SIZE);  // 取模 1024
}
```

举例：

```
seq_no = 2050
  → key = 2050 / 1024 = 2
  → offset = 2050 % 1024 = 2
  → 含义：在 bitSetMap[2] 这个 BitSet 的第 2 位
```

### 3.3 markSeqNo 流程

假设 checkpoint = 5，来了 seq_no = 8：

```java
private void markSeqNo(final long seqNo, final AtomicLong checkPoint, final Map<Long, CountedBitSet> bitSetMap) {
    advanceMaxSeqNo(seqNo);          // 更新已见过的最大 seq_no

    if (seqNo <= checkPoint.get()) { // 8 > 5，不走这里
        return;
    }

    // 找到 seq_no=8 所属的 BitSet，在对应 offset 处设为 1
    final CountedBitSet bitSet = getBitSetForSeqNo(bitSetMap, seqNo);
    final int offset = seqNoToBitSetOffset(seqNo);
    bitSet.set(offset);

    if (seqNo == checkPoint.get() + 1) {  // 8 == 5+1? 不等于，不推进
        updateCheckpoint(checkPoint, bitSetMap);
    }
}
```

关键：只有当 `seqNo == checkpoint + 1` 时才触发推进。

### 3.4 updateCheckpoint 推进过程

```java
private void updateCheckpoint(AtomicLong checkPoint, Map<Long, CountedBitSet> bitSetMap) {
    long bitSetKey = getBitSetKey(checkPoint.get());
    CountedBitSet current = bitSetMap.get(bitSetKey);

    do {
        checkPoint.incrementAndGet();

        // 如果 checkpoint 到了当前 bitset 的最后一位，清理整段并切到下一段
        if (checkPoint.get() == lastSeqNoInBitSet(bitSetKey)) {
            bitSetMap.remove(bitSetKey);
            current = bitSetMap.get(++bitSetKey);
        }
    } while (current != null && current.get(seqNoToBitSetOffset(checkPoint.get() + 1)));
    //        ↑ 还有下一段           ↑ 下一个位置也完成了吗？是就继续推
}
```

具体示例（用 BIT_SET_SIZE=8 简化）：

```
bitSetMap[key=0]:  位置 0~7 → [1,1,1,1,1,1,1,0]
checkpoint = 3

触发推进:
  checkpoint = 4, 检查位置5 → 1 → 继续
  checkpoint = 5, 检查位置6 → 1 → 继续
  checkpoint = 6, 检查位置7 → 0 → 停

结果: checkpoint = 6
```

跨 BitSet 边界示例：

```
bitSetMap[key=0]:  [1,1,1,1,1,1,1,1]   全满
bitSetMap[key=1]:  [1,1,0,...]          seq_no 8,9 完成，10 未完成
checkpoint = 6

推进:
  checkpoint = 7 → 到了 key=0 末尾 → 删除 bitSetMap[0]，切到 bitSetMap[1]
  checkpoint = 8, 检查位置1 → 1 → 继续
  checkpoint = 9, 检查位置2 → 0 → 停

结果: checkpoint = 9, bitSetMap[0] 已释放
```

### 3.5 设计精妙之处

| 设计 | 目的 |
|------|------|
| 分段 BitSet（每段 1024） | 避免分配巨大连续内存 |
| 用完即删（`bitSetMap.remove`） | checkpoint 左边的已不需要，释放内存 |
| 只在 `seqNo == checkpoint+1` 时推进 | 避免每次 markSeqNo 都做无用扫描 |
| `computeIfAbsent` 按需创建 | 操作乱序到达时，按需分配对应段 |

本质：一个**滑动窗口**。左边界是 checkpoint，右边界是最大 seq_no。窗口内用 BitSet 记录完成状态，窗口左边的段不断回收。

```
内存中的 bitSetMap:

  [key=0]  [key=1]  [key=2]  ...
  ┌──────┐ ┌──────┐ ┌──────┐
  │111111│ │1110..│ │0000..│
  └──────┘ └──────┘ └──────┘
   已完成    部分完成   未来

       ↑ checkpoint 推进到这里后，key=0 整段删除
```

## 四、这套机制与 CCR 的关系

CCR 的复制循环本质上就是：

```
Follower: "我的 local checkpoint 是 100，给我 seq_no > 100 的操作"
Leader:   "好的，这里是 101-200 的操作"
Follower: "写入完毕，我的 local checkpoint 现在是 200 了"
Follower: "给我 seq_no > 200 的操作"
Leader:   "目前 global checkpoint 也是 200，没有新操作，我等着...（长轮询）"
```

1. **seq_no** 给了每个操作一个全局唯一且有序的身份
2. **Local Checkpoint** 告诉 follower "我处理到哪了"
3. **Global Checkpoint** 告诉 leader "follower 已经确认到哪了"，决定长轮询何时返回

## 五、Q&A：副本的 seq_no 从哪来？

**Q：副本的 seq_no 是直接复制主分片的吗？**

A：是的。seq_no 由 primary 分配，replica 直接使用。Primary 执行写操作时分配 seq_no，然后把"操作 + seq_no"一起发给 replica。Replica 不会自己生成序号。

**Q：Replica 的复制工作是什么？**

A：按 primary 给的 seq_no 回放操作。回放完一个就在自己的 `LocalCheckpointTracker` 中 `markSeqNoAsProcessed(seqNo)`，推进自己的 local checkpoint。然后 primary 收集所有 replica 的 local checkpoint，取最小值更新 global checkpoint。

**Q：这和 CCR 的 FollowingEngine 有什么关系？**

A：CCR 的 follower 也是用 leader 的 seq_no 写入，行为类似 replica。但它又是自己集群的 primary shard，所以需要特殊引擎 `FollowingEngine` 来"以 primary 身份接受外部指定的 seq_no"。这个在第 17 课会详细讲。

## 六、总结

| 概念 | 类型 | 维护者 | 含义 |
|------|------|--------|------|
| seq_no | long (递增) | Primary shard 分配 | 每个写操作的编号 |
| Local Checkpoint | long | 每个 shard 自己 (`LocalCheckpointTracker`) | 该 shard 连续完成的最大 seq_no |
| Global Checkpoint | long | Primary shard (`ReplicationTracker`) | 所有 in-sync 副本都确认的最大 seq_no |
| Primary Term | long | Master 分配 | Primary 的代际编号，切主时 +1 |

核心比喻：
- seq_no 是 CCR 的**坐标系**——没有它，follower 无法表达"从哪开始读"
- Local Checkpoint 是**进度条**——follower 用它告诉自己和 leader "我到哪了"
- Global Checkpoint 是**安全水位线**——它之前的操作所有副本都有了，可以安全清理

---

## 涉及的源码文件

- `server/src/main/java/org/elasticsearch/index/seqno/SequenceNumbers.java`
- `server/src/main/java/org/elasticsearch/index/seqno/LocalCheckpointTracker.java`
- `server/src/main/java/org/elasticsearch/index/seqno/ReplicationTracker.java`
