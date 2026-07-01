/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * The node task that fetch the write operations from a leader shard and
 * persists these ops in the follower shard.
 * 以下六个方法需要重写
 * 1. 读操作 — 从 Leader 拉取新的写操作（Index、Delete）
 * 2. 写操作 — 把拉回来的操作写入 Follower 本地分片
 * 3. 同步 Mapping — Leader 加了新字段，Follower 也要加
 * 4. 同步 Settings — Leader 改了分片配置，Follower 也要改
 * 5. 同步 Aliases — Leader 加了别名，Follower 也要加
 * 6. 续期租约 — 定期告诉 Leader"别删我还没复制到的历史数据"
 */
public abstract class ShardFollowNodeTask extends AllocatedPersistentTask {

    private static final int DELAY_MILLIS = 50;
    private static final Logger LOGGER = LogManager.getLogger(ShardFollowNodeTask.class);

    private final ShardFollowTask params;
    private final BiConsumer<TimeValue, Runnable> scheduler;
    private final LongSupplier relativeTimeProvider;

    private String followerHistoryUUID;
    private long leaderGlobalCheckpoint;        // leader当前的GCP
    private long leaderMaxSeqNo;                // leader当前最大的seqNo
    private long leaderMaxSeqNoOfUpdatesOrDeletes = SequenceNumbers.UNASSIGNED_SEQ_NO;
    private long lastRequestedSeqNo;            // 请求的位置（不代表数据已经返回）
    private long followerGlobalCheckpoint = 0;  // follower的GCP
    private long followerMaxSeqNo = 0;          // follower当前最大的seqNo
    private int numOutstandingReads = 0;        // 当前有几个请求发出去还没回来
    private int numOutstandingWrites = 0;
    private long currentMappingVersion = 0;
    private long currentSettingsVersion = 0;
    private long currentAliasesVersion = 0;
    private long totalReadRemoteExecTimeMillis = 0;
    private long totalReadTimeMillis = 0;
    private long successfulReadRequests = 0;
    private long failedReadRequests = 0;
    private long operationsRead = 0;
    private long bytesRead = 0;
    private long totalWriteTimeMillis = 0;
    private long successfulWriteRequests = 0;
    private long failedWriteRequests = 0;
    private long operationWritten = 0;
    private long lastFetchTime = -1;
    // 上次没读完的范围，下次优先继续读取
    private final Queue<Tuple<Long, Long>> partialReadRequests = new PriorityQueue<>(Comparator.comparing(Tuple::v1));
    // 读回来但是还没有写入Follower的操作，放在按照seqNo排序的优先队列中
    private final Queue<Translog.Operation> buffer = new PriorityQueue<>(Comparator.comparing(Translog.Operation::seqNo));
    private long bufferSizeInBytes = 0;         // buffer当前的字节数
    private final LinkedHashMap<Long, Tuple<AtomicInteger, ElasticsearchException>> fetchExceptions;

    private volatile ElasticsearchException fatalException;

    private Scheduler.Cancellable renewable;

    synchronized Scheduler.Cancellable getRenewable() {
        return renewable;
    }

    ShardFollowNodeTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers,
        ShardFollowTask params,
        BiConsumer<TimeValue, Runnable> scheduler,
        final LongSupplier relativeTimeProvider
    ) {
        super(id, type, action, description, parentTask, headers);
        this.params = params;
        this.scheduler = scheduler;
        this.relativeTimeProvider = relativeTimeProvider;
        /*
         * We keep track of the most recent fetch exceptions, with the number of exceptions that we track equal to the maximum number of
         * concurrent fetches. For each failed fetch, we track the from sequence number associated with the request, and we clear the entry
         * when the fetch task associated with that from sequence number succeeds.
         */
        this.fetchExceptions = new LinkedHashMap<Long, Tuple<AtomicInteger, ElasticsearchException>>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Long, Tuple<AtomicInteger, ElasticsearchException>> eldest) {
                return size() > params.getMaxOutstandingReadRequests();
            }
        };
    }

    // 启动流程
    @SuppressWarnings("HiddenField")
    void start(
        final String followerHistoryUUID,
        final long leaderGlobalCheckpoint,
        final long leaderMaxSeqNo,
        final long followerGlobalCheckpoint,
        final long followerMaxSeqNo
    ) {
        /*
         * While this should only ever be called once and before any other threads can touch these fields, we use synchronization here to
         * avoid the need to declare these fields as volatile. That is, we are ensuring these fields are always accessed under the same
         * lock.
         */
        synchronized (this) {
            // 1. 初始化所有状态
            this.followerHistoryUUID = followerHistoryUUID;
            this.leaderGlobalCheckpoint = leaderGlobalCheckpoint;
            this.leaderMaxSeqNo = leaderMaxSeqNo;
            this.followerGlobalCheckpoint = followerGlobalCheckpoint;
            this.followerMaxSeqNo = followerMaxSeqNo;
            this.lastRequestedSeqNo = followerGlobalCheckpoint;
            // 2. 启动租约续期定时器
            renewable = scheduleBackgroundRetentionLeaseRenewal(() -> {
                synchronized (ShardFollowNodeTask.this) {
                    return this.followerGlobalCheckpoint;
                }
            });
        }

        // updates follower mapping, this gets us the leader mapping version and makes sure that leader and follower mapping are identical
        // 先同步mapping
        updateMapping(0L, leaderMappingVersion -> {
            synchronized (ShardFollowNodeTask.this) {
                currentMappingVersion = Math.max(currentMappingVersion, leaderMappingVersion);
            }
            // 再同步setting
            updateSettings(leaderSettingsVersion -> {
                synchronized (ShardFollowNodeTask.this) {
                    currentSettingsVersion = Math.max(currentSettingsVersion, leaderSettingsVersion);
                }
                // 再同步别名
                updateAliases(leaderAliasesVersion -> {
                    synchronized (ShardFollowNodeTask.this) {
                        currentAliasesVersion = Math.max(currentAliasesVersion, leaderAliasesVersion);
                        LOGGER.info(
                            "{} following leader shard {}, "
                                + "follower global checkpoint=[{}], "
                                + "mapping version=[{}], "
                                + "settings version=[{}], "
                                + "aliases version=[{}]",
                            params.getFollowShardId(),
                            params.getLeaderShardId(),
                            followerGlobalCheckpoint,
                            currentMappingVersion,
                            currentSettingsVersion,
                            currentAliasesVersion
                        );
                    }
                    // 真正开始读
                    coordinateReads();
                });
            });
        });
    }

    // 从 leader 读请求的调用中心
    synchronized void coordinateReads() {
        if (isStopped()) {
            LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            return;
        }

        LOGGER.trace(
            "{} coordinate reads, lastRequestedSeqNo={}, leaderGlobalCheckpoint={}",
            params.getFollowShardId(),
            lastRequestedSeqNo,
            leaderGlobalCheckpoint
        );
        assert partialReadRequests.size() <= params.getMaxOutstandingReadRequests()
            : "too many partial read requests [" + partialReadRequests + "]";
        // 第一部分：优先处理"上次没读完的范围"
        // 假如你请求"从 seqNo=100 开始读 1000 条"，但 Leader 只返回了 500 条，剩下的会被记录到 partialReadRequests 队列
        while (hasReadBudget() && partialReadRequests.isEmpty() == false) {
            final Tuple<Long, Long> range = partialReadRequests.remove();
            assert range.v1() <= range.v2() && range.v2() <= lastRequestedSeqNo
                : "invalid partial range [" + range.v1() + "," + range.v2() + "]; last requested seq_no [" + lastRequestedSeqNo + "]";
            final long fromSeqNo = range.v1();
            final long maxRequiredSeqNo = range.v2();
            final int requestOpCount = Math.toIntExact(maxRequiredSeqNo - fromSeqNo + 1);
            LOGGER.trace(
                "{}[{} ongoing reads] continue partial read request from_seqno={} max_required_seqno={} batch_count={}",
                params.getFollowShardId(),
                numOutstandingReads,
                fromSeqNo,
                maxRequiredSeqNo,
                requestOpCount
            );
            numOutstandingReads++;
            sendShardChangesRequest(fromSeqNo, requestOpCount, maxRequiredSeqNo);
        }
        final int maxReadRequestOperationCount = params.getMaxReadRequestOperationCount();
        // 第二部分：发起新的读请求，追赶 leaderGlobalCheckpoint
        while (hasReadBudget() && lastRequestedSeqNo < leaderGlobalCheckpoint) {
            final long from = lastRequestedSeqNo + 1;
            // 范围最大maxReadRequestOperationCount
            final long maxRequiredSeqNo = Math.min(leaderGlobalCheckpoint, from + maxReadRequestOperationCount - 1);
            final int requestOpCount;
            if (numOutstandingReads == 0) {
                // This is the only request, we can optimistically fetch more documents if possible but not enforce max_required_seqno.
                requestOpCount = maxReadRequestOperationCount;
            } else {
                requestOpCount = Math.toIntExact(maxRequiredSeqNo - from + 1);
            }
            assert 0 < requestOpCount && requestOpCount <= maxReadRequestOperationCount : "read_request_operation_count=" + requestOpCount;
            LOGGER.trace(
                "{}[{} ongoing reads] read from_seqno={} max_required_seqno={} batch_count={}",
                params.getFollowShardId(),
                numOutstandingReads,
                from,
                maxRequiredSeqNo,
                requestOpCount
            );
            numOutstandingReads++;
            lastRequestedSeqNo = maxRequiredSeqNo;
            sendShardChangesRequest(from, requestOpCount, maxRequiredSeqNo);
        }

        // 第三部分：已追上，发一个"探测请求"（长轮询）
        if (numOutstandingReads == 0 && hasReadBudget()) {
            assert lastRequestedSeqNo == leaderGlobalCheckpoint;
            // We sneak peek if there is any thing new in the leader.
            // If there is we will happily accept
            numOutstandingReads++;
            long from = lastRequestedSeqNo + 1;
            LOGGER.trace("{}[{}] peek read [{}]", params.getFollowShardId(), numOutstandingReads, from);
            sendShardChangesRequest(from, maxReadRequestOperationCount, lastRequestedSeqNo);
        }
    }

    // 三个限制条件：并发数，缓冲区字节数，缓冲区操作数
    private boolean hasReadBudget() {
        assert Thread.holdsLock(this);
        // TODO: To ensure that we never overuse the buffer, we need to
        // - Overestimate the size and count of the responses of the outstanding request when calculating the budget
        // - Limit the size and count of next read requests by the remaining size and count of the buffer
        if (numOutstandingReads >= params.getMaxOutstandingReadRequests()) {
            LOGGER.trace(
                "{} no new reads, maximum number of concurrent reads have been reached [{}]",
                params.getFollowShardId(),
                numOutstandingReads
            );
            return false;
        }
        if (bufferSizeInBytes >= params.getMaxWriteBufferSize().getBytes()) {
            LOGGER.trace("{} no new reads, buffer size limit has been reached [{}]", params.getFollowShardId(), bufferSizeInBytes);
            return false;
        }
        if (buffer.size() >= params.getMaxWriteBufferCount()) {
            LOGGER.trace("{} no new reads, buffer count limit has been reached [{}]", params.getFollowShardId(), buffer.size());
            return false;
        }
        return true;
    }

    // 写请求的调用中心
    private synchronized void coordinateWrites() {
        if (isStopped()) {
            LOGGER.info("{} shard follow task has been stopped", params.getFollowShardId());
            return;
        }

        // 有写预算且buffer不为空
        // 这里发送请求也是异步的，所以这个while循环相当于并发执行，里面都是在控制并发数和读取buffer的大小
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
            LOGGER.trace(
                "{}[{}] write [{}/{}] [{}]",
                params.getFollowShardId(),
                numOutstandingWrites,
                ops.get(0).seqNo(),
                ops.get(ops.size() - 1).seqNo(),
                ops.size()
            );
            sendBulkShardOperationsRequest(ops, leaderMaxSeqNoOfUpdatesOrDeletes, new AtomicInteger(0));
        }
    }

    private boolean hasWriteBudget() {
        assert Thread.holdsLock(this);
        if (numOutstandingWrites >= params.getMaxOutstandingWriteRequests()) {
            LOGGER.trace("{} maximum number of concurrent writes have been reached [{}]", params.getFollowShardId(), numOutstandingWrites);
            return false;
        }
        return true;
    }

    private void sendShardChangesRequest(long from, int maxOperationCount, long maxRequiredSeqNo) {
        sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, new AtomicInteger(0));
    }

    // 发送读请求
    private void sendShardChangesRequest(long from, int maxOperationCount, long maxRequiredSeqNo, AtomicInteger retryCounter) {
        // 记录开始时间
        final long startTime = relativeTimeProvider.getAsLong();
        synchronized (this) {
            lastFetchTime = startTime;
        }
        innerSendShardChangesRequest(from, maxOperationCount, response -> {
            // 成功回调
            synchronized (ShardFollowNodeTask.this) {
                // Always clear fetch exceptions:
                fetchExceptions.remove(from);
                if (response.getOperations().length > 0) {
                    // do not count polls against fetch stats
                    totalReadRemoteExecTimeMillis += response.getTookInMillis();
                    totalReadTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                    successfulReadRequests++;
                    operationsRead += response.getOperations().length;
                    bytesRead += Arrays.stream(response.getOperations()).mapToLong(Translog.Operation::estimateSize).sum();
                }
            }
            handleReadResponse(from, maxRequiredSeqNo, response);
        }, e -> {
            // 失败回调
            synchronized (ShardFollowNodeTask.this) {
                totalReadTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                failedReadRequests++;
                fetchExceptions.put(from, Tuple.tuple(retryCounter, ExceptionsHelper.convertToElastic(e)));
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            // 请求的操作已经被merge了
            if (cause instanceof ResourceNotFoundException resourceNotFoundException) {
                if (resourceNotFoundException.getMetadataKeys().contains(Ccr.REQUESTED_OPS_MISSING_METADATA_KEY)) {
                    handleFallenBehindLeaderShard(e, from, maxOperationCount, maxRequiredSeqNo, retryCounter);
                    return;
                }
            }
            handleFailure(e, retryCounter, () -> sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, retryCounter));
        });
    }

    void handleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        // In order to process this read response (3), we need to check and potentially update the follow index's setting (1) and
        // check and potentially update the follow index's mappings (2).

        // 执行顺序从下往上，上一个回调函数作为下一个的参数
        // 4) handle read response:
        Runnable handleResponseTask = () -> innerHandleReadResponse(from, maxRequiredSeqNo, response);
        // 3) update follow index mapping:
        Runnable updateMappingsTask = () -> maybeUpdateMapping(response.getMappingVersion(), handleResponseTask);
        // 2) update follow index settings:
        Runnable updateSettingsTask = () -> maybeUpdateSettings(response.getSettingsVersion(), updateMappingsTask);
        // 1) update follow index aliases:
        maybeUpdateAliases(response.getAliasesVersion(), updateSettingsTask);
    }

    // 请求的seqNo已经被merge掉了，按理说只能重新bootstrap一遍。但是这里注释显示还是会一直重试，后续需要优化
    void handleFallenBehindLeaderShard(Exception e, long from, int maxOperationCount, long maxRequiredSeqNo, AtomicInteger retryCounter) {
        // Do restore from repository here and after that
        // start() should be invoked and stats should be reset

        // For now handle like any other failure:
        // need a more robust approach to avoid the scenario where an outstanding request
        // can trigger another restore while the shard was restored already.
        // https://github.com/elastic/elasticsearch/pull/37562#discussion_r250009367

        handleFailure(e, retryCounter, () -> sendShardChangesRequest(from, maxOperationCount, maxRequiredSeqNo, retryCounter));
    }

    /** Called when some operations are fetched from the leading */
    protected void onOperationsFetched(Translog.Operation[] operations) {

    }

    synchronized void innerHandleReadResponse(long from, long maxRequiredSeqNo, ShardChangesAction.Response response) {
        onOperationsFetched(response.getOperations());
        // 1. 更新 Leader 的全局状态
        leaderGlobalCheckpoint = Math.max(leaderGlobalCheckpoint, response.getGlobalCheckpoint());
        leaderMaxSeqNo = Math.max(leaderMaxSeqNo, response.getMaxSeqNo());
        leaderMaxSeqNoOfUpdatesOrDeletes = SequenceNumbers.max(leaderMaxSeqNoOfUpdatesOrDeletes, response.getMaxSeqNoOfUpdatesOrDeletes());
        final long newFromSeqNo;
        if (response.getOperations().length == 0) {
            // 返回空，这段时间内没有新数据，继续发 coordinateReads
            newFromSeqNo = from;
        } else {
            assert response.getOperations()[0].seqNo() == from
                : "first operation is not what we asked for. From is [" + from + "], got " + response.getOperations()[0];
            List<Translog.Operation> operations = Arrays.asList(response.getOperations());
            long operationsSize = operations.stream().mapToLong(Translog.Operation::estimateSize).sum();
            // 2. 把操作放入buffer
            buffer.addAll(operations);
            bufferSizeInBytes += operationsSize;
            final long maxSeqNo = response.getOperations()[response.getOperations().length - 1].seqNo();
            assert maxSeqNo == Arrays.stream(response.getOperations()).mapToLong(Translog.Operation::seqNo).max().getAsLong();
            newFromSeqNo = maxSeqNo + 1;
            // update last requested seq no as we may have gotten more than we asked for and we don't want to ask it again.
            // 3. 更新 lastRequestedSeqNo（可能收到的比请求的多）
            lastRequestedSeqNo = Math.max(lastRequestedSeqNo, maxSeqNo);
            assert lastRequestedSeqNo <= leaderGlobalCheckpoint
                : "lastRequestedSeqNo [" + lastRequestedSeqNo + "] is larger than the global checkpoint [" + leaderGlobalCheckpoint + "]";
            // 4. 触发写入
            coordinateWrites();
        }
        // 5. 如果这次没读完（newFromSeqNo <= maxRequiredSeqNo），记录为 partial
        if (newFromSeqNo <= maxRequiredSeqNo) {
            LOGGER.trace(
                "{} received [{}] operations, enqueue partial read request [{}/{}]",
                params.getFollowShardId(),
                response.getOperations().length,
                newFromSeqNo,
                maxRequiredSeqNo
            );
            partialReadRequests.add(Tuple.tuple(newFromSeqNo, maxRequiredSeqNo));
        }
        // 6. 释放一个读请求名额，继续调度
        numOutstandingReads--;
        coordinateReads();
    }

    // 发送写请求
    private void sendBulkShardOperationsRequest(
        List<Translog.Operation> operations,
        long leaderMaxSequenceNoOfUpdatesOrDeletes,
        AtomicInteger retryCounter
    ) {
        assert leaderMaxSequenceNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "mus is not replicated";
        final long startTime = relativeTimeProvider.getAsLong();
        innerSendBulkShardOperationsRequest(followerHistoryUUID, operations, leaderMaxSequenceNoOfUpdatesOrDeletes, response -> {
            // 成功回调
            synchronized (ShardFollowNodeTask.this) {
                totalWriteTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                successfulWriteRequests++;
                operationWritten += operations.size();
            }
            handleWriteResponse(response);
        }, e -> {
            // 失败回调
            synchronized (ShardFollowNodeTask.this) {
                totalWriteTimeMillis += TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - startTime);
                failedWriteRequests++;
            }
            handleFailure(
                e,
                retryCounter,
                () -> sendBulkShardOperationsRequest(operations, leaderMaxSequenceNoOfUpdatesOrDeletes, retryCounter)
            );
        });
    }

    private synchronized void handleWriteResponse(final BulkShardOperationsResponse response) {
        // 写成功后更新GCP和maxSeqNo
        this.followerGlobalCheckpoint = Math.max(this.followerGlobalCheckpoint, response.getGlobalCheckpoint());
        this.followerMaxSeqNo = Math.max(this.followerMaxSeqNo, response.getMaxSeqNo());
        numOutstandingWrites--;
        assert numOutstandingWrites >= 0;
        // 继续递归到写请求
        coordinateWrites();

        // In case that buffer has more ops than is allowed then reads may all have been stopped,
        // this invocation makes sure that we start a read when there is budget in case no reads are being performed.
        // 递归到读请求
        coordinateReads();
    }

    private synchronized void maybeUpdateMapping(long minimumRequiredMappingVersion, Runnable task) {
        if (currentMappingVersion >= minimumRequiredMappingVersion) {
            LOGGER.trace(
                "{} mapping version [{}] is higher or equal than minimum required mapping version [{}]",
                params.getFollowShardId(),
                currentMappingVersion,
                minimumRequiredMappingVersion
            );
            task.run();
        } else {
            LOGGER.trace(
                "{} updating mapping, mapping version [{}] is lower than minimum required mapping version [{}]",
                params.getFollowShardId(),
                currentMappingVersion,
                minimumRequiredMappingVersion
            );
            updateMapping(minimumRequiredMappingVersion, mappingVersion -> {
                synchronized (ShardFollowNodeTask.this) {
                    currentMappingVersion = Math.max(currentMappingVersion, mappingVersion);
                }
                task.run();
            });
        }
    }

    private synchronized void maybeUpdateSettings(final Long minimumRequiredSettingsVersion, Runnable task) {
        if (currentSettingsVersion >= minimumRequiredSettingsVersion) {
            LOGGER.trace(
                "{} settings version [{}] is higher or equal than minimum required settings version [{}]",
                params.getFollowShardId(),
                currentSettingsVersion,
                minimumRequiredSettingsVersion
            );
            task.run();
        } else {
            LOGGER.trace(
                "{} updating settings, settings version [{}] is lower than minimum required settings version [{}]",
                params.getFollowShardId(),
                currentSettingsVersion,
                minimumRequiredSettingsVersion
            );
            updateSettings(settingsVersion -> {
                synchronized (ShardFollowNodeTask.this) {
                    currentSettingsVersion = Math.max(currentSettingsVersion, settingsVersion);
                }
                task.run();
            });
        }
    }

    private synchronized void maybeUpdateAliases(final Long minimumRequiredAliasesVersion, final Runnable task) {
        if (currentAliasesVersion >= minimumRequiredAliasesVersion) {
            LOGGER.trace(
                "{} aliases version [{}] is higher or equal than minimum required aliases version [{}]",
                params.getFollowShardId(),
                currentAliasesVersion,
                minimumRequiredAliasesVersion
            );
            task.run();
        } else {
            LOGGER.trace(
                "{} updating aliases, aliases version [{}] is lower than minimum required aliases version [{}]",
                params.getFollowShardId(),
                currentAliasesVersion,
                minimumRequiredAliasesVersion
            );
            updateAliases(aliasesVersion -> {
                synchronized (ShardFollowNodeTask.this) {
                    currentAliasesVersion = Math.max(currentAliasesVersion, aliasesVersion);
                }
                task.run();
            });
        }
    }

    private void updateMapping(long minRequiredMappingVersion, LongConsumer handler) {
        updateMapping(minRequiredMappingVersion, handler, new AtomicInteger(0));
    }

    private void updateMapping(long minRequiredMappingVersion, LongConsumer handler, AtomicInteger retryCounter) {
        innerUpdateMapping(
            minRequiredMappingVersion,
            handler,
            e -> handleFailure(e, retryCounter, () -> updateMapping(minRequiredMappingVersion, handler, retryCounter))
        );
    }

    private void updateSettings(final LongConsumer handler) {
        updateSettings(handler, new AtomicInteger(0));
    }

    private void updateSettings(final LongConsumer handler, final AtomicInteger retryCounter) {
        innerUpdateSettings(handler, e -> handleFailure(e, retryCounter, () -> updateSettings(handler, retryCounter)));
    }

    private void updateAliases(final LongConsumer handler) {
        updateAliases(handler, new AtomicInteger());
    }

    private void updateAliases(final LongConsumer handler, final AtomicInteger retryCounter) {
        innerUpdateAliases(handler, e -> handleFailure(e, retryCounter, () -> updateAliases(handler, retryCounter)));
    }

    // 失败处理，要分清可重试和不可重试的
    private void handleFailure(Exception e, AtomicInteger retryCounter, Runnable task) {
        assert e != null;
        if (shouldRetry(e)) {
            if (isStopped() == false) {
                // Only retry is the shard follow task is not stopped.
                int currentRetry = retryCounter.incrementAndGet();
                LOGGER.debug(() -> format("%s error during follow shard task, retrying [%s]", params.getFollowShardId(), currentRetry), e);
                // 指数退避重试
                long delay = computeDelay(currentRetry, params.getReadPollTimeout().getMillis());
                scheduler.accept(TimeValue.timeValueMillis(delay), task);
            }
        } else {
            onFatalFailure(e);
        }
    }

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

    // 指数退避重试
    static long computeDelay(int currentRetry, long maxRetryDelayInMillis) {
        // Cap currentRetry to avoid overflow when computing n variable
        int maxCurrentRetry = Math.min(currentRetry, 24);
        long n = Math.round(Math.pow(2, maxCurrentRetry - 1));
        // + 1 here, because nextInt(...) bound is exclusive and otherwise the first delay would always be zero.
        int k = Randomness.get().nextInt(Math.toIntExact(n + 1));
        int backOffDelay = k * DELAY_MILLIS;
        return Math.min(backOffDelay, maxRetryDelayInMillis);
    }

    static boolean shouldRetry(final Exception e) {
        if (NetworkExceptionHelper.isConnectException(e)
            || NetworkExceptionHelper.getCloseConnectionExceptionLevel(e, false) != Level.OFF) {
            return true;
        }

        /**
         * 可重试的（都是暂时性的）：
         *   ┌─────────────────────────────────┬────────────────────────────────────────────┐
         *   │            错误类型              │                  典型场景                    │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ 网络连接异常                      │ Leader 暂时不可达                            │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ ShardNotFoundException          │ 分片正在迁移                                 │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ IllegalIndexShardStateException │ 分片正在恢复中                               │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ NoShardAvailableActionException │ 分片暂时没有可用副本                          │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ AlreadyClosedException          │ 分片正在关闭（relocate）                      │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ ElasticsearchSecurityException  │ 权限暂时不可用（证书轮换中）                    │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ ClusterBlockException           │ Leader 索引被临时 block 或无 master           │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ IndexClosedException            │ Follower 索引被关闭（updateSettings 期间）    │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ ConnectTransportException       │ 传输层连接失败                               │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ NodeClosedException             │ 目标节点正在重启                              │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ NoSuchRemoteClusterException    │ 远程集群配置暂时不可用                         │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ EsRejectedExecutionException    │ 线程池满了                                   │
         *   ├─────────────────────────────────┼────────────────────────────────────────────┤
         *   │ CircuitBreakingException        │ 内存熔断                                     │
         *   └─────────────────────────────────┴────────────────────────────────────────────┘
         */
        final Throwable actual = ExceptionsHelper.unwrapCause(e);
        return actual instanceof ShardNotFoundException
            || actual instanceof IllegalIndexShardStateException
            || actual instanceof NoShardAvailableActionException
            || actual instanceof UnavailableShardsException
            || actual instanceof AlreadyClosedException
            || actual instanceof ElasticsearchSecurityException // If user does not have sufficient privileges
            || actual instanceof ClusterBlockException // If leader index is closed or no elected master
            || actual instanceof IndexClosedException // If follow index is closed
            || actual instanceof ConnectTransportException
            || actual instanceof NodeClosedException
            || actual instanceof NoSuchRemoteClusterException
            || actual instanceof NoSeedNodeLeftException
            || actual instanceof EsRejectedExecutionException
            || actual instanceof CircuitBreakingException;
    }

    // These methods are protected for testing purposes:
    protected abstract void innerUpdateMapping(long minRequiredMappingVersion, LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerUpdateSettings(LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerUpdateAliases(LongConsumer handler, Consumer<Exception> errorHandler);

    protected abstract void innerSendBulkShardOperationsRequest(
        String followerHistoryUUID,
        List<Translog.Operation> operations,
        long leaderMaxSeqNoOfUpdatesOrDeletes,
        Consumer<BulkShardOperationsResponse> handler,
        Consumer<Exception> errorHandler
    );

    protected abstract void innerSendShardChangesRequest(
        long from,
        int maxOperationCount,
        Consumer<ShardChangesAction.Response> handler,
        Consumer<Exception> errorHandler
    );

    protected abstract Scheduler.Cancellable scheduleBackgroundRetentionLeaseRenewal(LongSupplier followerGlobalCheckpoint);

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

    protected boolean isStopped() {
        return fatalException != null || isCancelled() || isCompleted();
    }

    public ShardId getFollowShardId() {
        return params.getFollowShardId();
    }

    @Override
    public synchronized ShardFollowNodeTaskStatus getStatus() {
        final long timeSinceLastFetchMillis;
        if (lastFetchTime != -1) {
            timeSinceLastFetchMillis = TimeUnit.NANOSECONDS.toMillis(relativeTimeProvider.getAsLong() - lastFetchTime);
        } else {
            // To avoid confusion when ccr didn't yet execute a fetch:
            timeSinceLastFetchMillis = -1;
        }
        return new ShardFollowNodeTaskStatus(
            params.getRemoteCluster(),
            params.getLeaderShardId().getIndexName(),
            params.getFollowShardId().getIndexName(),
            getFollowShardId().getId(),
            leaderGlobalCheckpoint,
            leaderMaxSeqNo,
            followerGlobalCheckpoint,
            followerMaxSeqNo,
            lastRequestedSeqNo,
            numOutstandingReads,
            numOutstandingWrites,
            buffer.size(),
            bufferSizeInBytes,
            currentMappingVersion,
            currentSettingsVersion,
            currentAliasesVersion,
            totalReadTimeMillis,
            totalReadRemoteExecTimeMillis,
            successfulReadRequests,
            failedReadRequests,
            operationsRead,
            bytesRead,
            totalWriteTimeMillis,
            successfulWriteRequests,
            failedWriteRequests,
            operationWritten,
            new TreeMap<>(
                fetchExceptions.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Tuple.tuple(e.getValue().v1().get(), e.getValue().v2())))
            ),
            timeSinceLastFetchMillis,
            fatalException
        );
    }

}
