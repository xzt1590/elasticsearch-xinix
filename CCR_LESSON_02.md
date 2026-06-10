# 第 2 课：Soft Deletes 与历史操作保留

## 一、核心问题

CCR 第二阶段需要从 Leader 读取历史操作（seq_no X 到 Y 之间发生了什么）。这些历史操作从哪来？

- **Translog**：flush 后会截断，不能长期保留
- **Lucene 索引**：传统模式下，旧文档在 merge 时被物理清除，历史丢失

答案：**Soft Deletes**——让被覆盖/删除的文档物理保留在 Lucene 中，直到保留策略允许清除。

## 二、硬删除 vs 软删除的本质区别

区别不在于"删不删"，而在于 **merge 时谁说了算**：

- **硬删除**：merge 时，只要文档的 liveDocs bit = 0，就**无条件物理清除**，没有任何机制能阻止
- **软删除**：merge 时，会先问 `SoftDeletesRetentionMergePolicy` 的 Retention Query："这个文档需要保留吗？"只有不匹配的才清除

```
硬删除: merge → 删，没有商量
软删除: merge → 先问 SoftDeletesPolicy → 回答"留着" → 不删
                                       → 回答"可以清" → 删
```

软删除给了 ES 一个**可控的保留窗口**。不是永远不删，是"我说可以删的时候才删"。

## 三、Lucene 搜索的背景知识：为什么需要标记旧文档

Lucene 搜索的工作方式和 LSM tree 不同。LSM tree 读取时做 merge-sort，自动用新版本覆盖旧版本。

Lucene 对每个 segment **独立搜索，不会自动按 id 去重**。假设：

```
Segment-1: doc(id=1, content="hello")
Segment-2: doc(id=1, content="world")    ← 更新后写入新 segment
```

如果不做任何标记，搜索 `content:*` 会返回两条结果——因为 Lucene 对每个 segment 独立执行查询，最后合并的是结果列表，不是按 id 去重。

所以**必须**在 Segment-1 中标记旧文档"已死"，搜索 Segment-1 时才会跳过它。

## 四、两套标记机制：liveDocs 与 __soft_deletes

ES 中存在两套标记，各有职责：

### 4.1 liveDocs（控制搜索可见性）

每个 segment 有一个 `.liv` 文件（bit vector），每个文档一个 bit：
- 1 = 活着（搜索可见）
- 0 = 已删除（搜索不可见）

`.liv` 文件是 segment 级别唯一**可以重新生成**的东西。它是分代的（generational），文件名类似 `_0_1.liv`、`_0_2.liv`，每次有文档被标记删除就生成新一代。

### 4.2 `__soft_deletes` 字段（控制 merge 是否物理清除）

```java
// Lucene.java:97
public static final String SOFT_DELETES_FIELD = "__soft_deletes";

// Lucene.java:948 — 值为 1 表示被软删除
public static NumericDocValuesField newSoftDeletesField() {
    return new NumericDocValuesField(SOFT_DELETES_FIELD, 1);
}
```

- 类型：`NumericDocValuesField`（列式存储，不是 stored field）
- 语义：**没有这个字段 = 活着**，**值为 1 = 被 soft-deleted**
- 不是 0 和 1 的区别，是"有没有这个字段"的区别

### 4.3 两套标记的关系

```
liveDocs (bit=0):       控制正常搜索是否可见。
__soft_deletes (=1):    告诉 merge 策略"这个文档虽然搜索不到了，但可能还有人需要读它的历史"。

正常搜索: 只看 liveDocs → 旧文档不可见
CCR 读取: 用 wrapAllDocsLive() 忽略 liveDocs → soft-deleted 文档可见，能读到历史
Merge 时: 看 __soft_deletes + retention query → 决定是否物理清除
```

**注意：ES 实际上禁用了硬删除**。`ElasticsearchIndexWriter` 中 `deleteDocuments` 方法直接抛异常：

```java
@Override
public long deleteDocuments(Term... terms) throws IOException {
    if (Assertions.ENABLED) {
        throw new AssertionError("must not hard delete documents");
    }
    return super.deleteDocuments(terms);
}
```

所有的"删除"都通过 `softUpdateDocument` 实现。

## 五、ES 中三种写操作的具体行为

### 5.1 更新文档

```java
// InternalEngine.java:1601
indexWriter.softUpdateDocument(uidTerm, newDoc, softDeletesField);
```

`softUpdateDocument` 的原子语义：
1. 对 Segment-1 中匹配 `uidTerm` 的旧文档，施加 doc values update：设置 `__soft_deletes=1`
2. 将新文档写入当前活跃 segment

```
Segment-1:  doc(id=1, seq_no=5, content="hello", __soft_deletes=1)  ← 被标记
Segment-2:  doc(id=1, seq_no=8, content="world")                    ← 新文档
```

### 5.2 删除文档

```java
// InternalEngine.java:1826-1830
doc.add(softDeletesField);  // 墓碑文档自身也标记为 soft-deleted
indexWriter.softUpdateDocument(new Term(IdFieldMapper.NAME, delete.uid()), doc, softDeletesField);
```

原子操作：
1. 旧文档标记 `__soft_deletes=1`
2. 写入一个墓碑文档（tombstone），墓碑自身也是 soft-deleted

### 5.3 为什么删除操作需要墓碑文档

墓碑是**"删除这个动作本身"的记录**。如果只标记旧文档为 soft-deleted，CCR 读取时**无法区分**这个文档是被删除了还是被更新了。

具体例子：

```
场景: 文档 id=1 经历了 写入→更新→删除

seq_no=1: INDEX  id=1 "hello"
seq_no=5: INDEX  id=1 "world"   (更新)
seq_no=9: DELETE id=1

Lucene 中实际存在的文档:

  doc_A: id=1, seq_no=1, content="hello", __soft_deletes=1  ← 被 seq_no=5 覆盖时标记
  doc_B: id=1, seq_no=5, content="world", __soft_deletes=1  ← 被 seq_no=9 删除时标记
  doc_C: id=1, seq_no=9, _tombstone=true, __soft_deletes=1  ← 墓碑，记录删除动作

正常搜索: 看不到任何东西（id=1 已被删除）
CCR 读取 seq_no 1~9: 能读到三个操作 → Index("hello"), Index("world"), Delete
```

墓碑文档没有业务内容（没有 `_source`），只有元数据（id、seq_no、primary_term、tombstone 标记）。它自身也是 soft-deleted，因为不应该出现在正常搜索结果中。

### 5.4 为什么更新操作也需要软删除

不只是删除需要，更新也需要软删除来保留旧版本：

```
seq_no=1: INSERT id=1 "hello"
seq_no=5: UPDATE id=1 "world"
seq_no=9: UPDATE id=1 "final"
```

假设 follower 的 bootstrap 恢复到 global checkpoint = 3，第二阶段从 seq_no=4 开始读。follower 需要读到 seq_no=5 的操作。但 seq_no=5 对应的文档（content="world"）已经被 seq_no=9 覆盖了。

如果没有软删除 → merge 后该文档消失 → CCR 读取 seq_no=5 时拿不到数据。

纯插入且之后从未被更新/删除的文档不需要软删除（它还活着）。但现实中大多数索引都有更新和删除。

## 六、Segment 不可变，标记怎么写入

Lucene segment 主数据文件不可变。`softUpdateDocument` 通过 **doc values update** 机制对旧 segment 中的文档施加更新。

### 6.1 四个阶段

**阶段一：写入时（内存中）**

```
                     IndexWriter 内存
                     ┌─────────────────────────────┐
Segment-1 (磁盘):   │  pending updates 队列:       │
  doc0: id=1 hello  │    "对 Segment-1 的 doc0,    │
  doc1: id=2 world  │     设置 __soft_deletes=1"   │
                     └─────────────────────────────┘

当前活跃 segment 的内存 buffer:
  newDoc: id=1 goodbye  ← 新文档先写到内存
```

此时磁盘上没有任何变化。更新请求只在 IndexWriter 的 RAM buffer 中。

**阶段二：Refresh（Reader Reopen）时**

ES 的 refresh 操作调用 `DirectoryReader.openIfChanged()`：
- Lucene 发现 Segment-1 有 pending doc values update
- 为 Segment-1 创建新的 SegmentReader，合并内存中的 update
- `LazySoftDeletesDirectoryReaderWrapper` 根据 `__soft_deletes` 重算 liveDocs
- 新 reader 中旧文档被排除

**这一步不写磁盘**。更新还在内存中，只是新 reader 能看到了。

**阶段三：Flush/Commit 时**

pending updates 落盘，为 segment 生成补充文件：

```
Segment-1 相关文件:
  _0.si          ← 不变（segment info）
  _0.cfs/.cfe    ← 不变（主数据）
  _0_2.liv       ← 新生成（新一代 liveDocs bit vector）
  _0_1.fnm       ← 新生成（字段信息更新）
  _0_1.dvd       ← 新生成（doc values 数据：哪些 doc 的 __soft_deletes=1）
  _0_1.dvm       ← 新生成（doc values 元数据）
```

后缀中的 `_1`、`_2` 是"代"数。每次有新的 update，代数递增。

**阶段四：Merge 时**

多个 segment 合并为一个新 segment：
- 所有补充信息被"烘焙"进新 segment 的主文件
- 不再有独立的 `.dvd`/`.dvm` 补充文件
- soft-deleted 且超出保留策略的文档被物理清除（真正消失）

```
Merge前:
  Segment-1: _0.cfs + _0_1.dvd + _0_1.dvm + _0_2.liv  （有补充文件）
  Segment-2: _1.cfs                                     （新 segment，干净）

Merge后:
  Segment-3: _2.cfs  ← 一个干净的新 segment，所有值在主文件里
             无补充文件
             过期的 soft-deleted 文档已物理删除
```

### 6.2 关于补充文件的读取

SegmentReader 创建时（refresh 触发）一次性加载 doc values（包括补充文件的 update），合并到内存中的 doc values 视图。之后查询直接读内存视图，不会每次查询都重新读补充文件。

实际上这些数据通过 mmap 映射，由 OS page cache 管理：热数据常驻内存，冷数据按需从磁盘加载。

## 七、从软删除中读取历史操作

### 7.1 wrapAllDocsLive —— 让 soft-deleted 文档重新可见

```java
// SearchBasedChangesSnapshot.java:247
static IndexSearcher newIndexSearcher(Engine.Searcher engineSearcher) throws IOException {
    return new IndexSearcher(Lucene.wrapAllDocsLive(engineSearcher.getDirectoryReader()));
}
```

`wrapAllDocsLive` 的作用：让所有 soft-deleted 的文档重新"可见"（只排除因异常导致的硬删除文档），这样 searcher 就能搜到历史操作了。

### 7.2 按 seq_no 范围查询

```java
// SearchBasedChangesSnapshot.java:250
static Query rangeQuery(long fromSeqNo, long toSeqNo, IndexVersion indexVersionCreated) {
    return new BooleanQuery.Builder()
        .add(LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, fromSeqNo, toSeqNo), BooleanClause.Occur.MUST)
        .add(Queries.newNonNestedFilter(indexVersionCreated), BooleanClause.Occur.MUST)
        .build();
}
```

找出 `_seq_no` 在 `[fromSeqNo, toSeqNo]` 范围内的所有文档（包括 soft-deleted 的），按 seq_no 升序排列，分批返回（默认每批 1024 个）。

### 7.3 文档还原为操作

`LuceneChangesSnapshot.readDocAsOp()` 根据 `isTombstone` 字段判断操作类型：

```java
if (isTombstone && fields.id() == null) {
    op = new Translog.NoOp(seqNo, primaryTerm, ...);      // NoOp 操作
} else if (isTombstone) {
    op = new Translog.Delete(id, seqNo, primaryTerm, ...); // 删除操作
} else {
    op = new Translog.Index(id, seqNo, primaryTerm, ...);  // 写入/更新操作
}
```

## 八、清理策略（SoftDeletesPolicy）

### 8.1 保留下限的计算

`SoftDeletesPolicy.getMinRetainedSeqNo()` 由三个因素决定：

```java
// 1. Retention Leases 中最小的 retainingSequenceNumber（CCR/peer-recovery 显式要求）
final long minimumRetainingSequenceNumber = retentionLeases.leases()
    .stream()
    .mapToLong(RetentionLease::retainingSequenceNumber)
    .min()
    .orElse(Long.MAX_VALUE);

// 2. global checkpoint - retentionOperations（配置的额外保留数）
final long minSeqNoForQueryingChanges = Math.min(
    1 + globalCheckpointSupplier.getAsLong() - retentionOperations,
    minimumRetainingSequenceNumber
);

// 3. safe commit 的 local checkpoint（peer recovery 需要）
final long minSeqNoToRetain = Math.min(minSeqNoForQueryingChanges, 1 + localCheckpointOfSafeCommit);
```

取三者最小值，`seq_no >= minRetainedSeqNo` 的 soft-deleted 文档保留，更早的可以在 merge 时物理清除。

### 8.2 Merge 时的 Retention Query

```java
// SoftDeletesPolicy.java:150
Query getRetentionQuery() {
    return LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, getMinRetainedSeqNo(), Long.MAX_VALUE);
}
```

这个 query 被用在 `SoftDeletesRetentionMergePolicy` 中。匹配的 soft-deleted 文档保留，不匹配的物理清除。

### 8.3 资源压力

不是"删除文档多"导致问题，而是"**删除文档多 + 保留策略不让清理**"才会有问题：
- CCR follower 严重落后 → retention lease 卡在很低的 seq_no → leader 大量旧文档不能清理
- `index.soft_deletes.retention.operations` 设置过大

如果 merge 能正常清理，soft-deleted 文档很快消失，和传统硬删除的最终效果一样。

## 九、数据流总结

```
用户写入/更新/删除
       │
       ▼
InternalEngine
  ├── index(): 新文档写入
  ├── softUpdateDocument(): 旧文档标记 soft-deleted + 新文档写入
  └── deleteInLucene(): 旧文档标记 soft-deleted + 墓碑写入
       │
       ▼
Lucene 索引: 活跃文档 + soft-deleted 文档（物理共存）
       │
       ├── 正常搜索: liveDocs 排除 soft-deleted → 用户只看到最新数据
       │
       ├── CCR 读取: wrapAllDocsLive() + rangeQuery(from, to) → 读出历史操作
       │
       └── Merge: SoftDeletesRetentionMergePolicy → 按策略清除过期的 soft-deleted 文档
```

---

## 涉及的源码文件

- `server/src/main/java/org/elasticsearch/common/lucene/Lucene.java` — `SOFT_DELETES_FIELD` 定义、`wrapAllDocsLive()`、`newSoftDeletesField()`
- `server/src/main/java/org/elasticsearch/index/engine/InternalEngine.java` — `softUpdateDocument` 调用、`ElasticsearchIndexWriter` 禁用硬删除
- `server/src/main/java/org/elasticsearch/index/engine/LuceneChangesSnapshot.java` — 按 seq_no 读取历史操作、`readDocAsOp()`
- `server/src/main/java/org/elasticsearch/index/engine/SearchBasedChangesSnapshot.java` — `wrapAllDocsLive()`、rangeQuery、分批机制
- `server/src/main/java/org/elasticsearch/index/engine/SoftDeletesPolicy.java` — 保留策略计算、retention query
- `server/src/main/java/org/elasticsearch/index/engine/LazySoftDeletesDirectoryReaderWrapper.java` — 从 `__soft_deletes` doc values 计算 liveDocs
