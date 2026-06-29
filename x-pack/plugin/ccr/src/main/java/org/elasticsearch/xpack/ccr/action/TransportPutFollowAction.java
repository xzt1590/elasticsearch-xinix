/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStream.BACKING_INDEX_PREFIX;
import static org.elasticsearch.xpack.ccr.Ccr.CCR_THREAD_POOL_NAME;

public final class TransportPutFollowAction extends TransportMasterNodeAction<PutFollowAction.Request, PutFollowAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutFollowAction.class);

    private final IndexScopedSettings indexScopedSettings;
    private final Client client;
    private final Executor remoteClientResponseExecutor;
    private final RestoreService restoreService;
    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportPutFollowAction(
        final ThreadPool threadPool,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndexScopedSettings indexScopedSettings,
        final ActionFilters actionFilters,
        final Client client,
        final RestoreService restoreService,
        final CcrLicenseChecker ccrLicenseChecker
    ) {
        super(
            PutFollowAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutFollowAction.Request::new,
            PutFollowAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexScopedSettings = indexScopedSettings;
        this.client = client;
        this.remoteClientResponseExecutor = threadPool.executor(CCR_THREAD_POOL_NAME);
        this.restoreService = restoreService;
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected void masterOperation(
        Task task,
        final PutFollowAction.Request request,
        final ClusterState state,
        final ActionListener<PutFollowAction.Response> listener
    ) {
        // 检查许可证
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }
        String remoteCluster = request.getRemoteCluster();
        // 验证远程集群能否正确连接
        client.getRemoteClusterClient(
            remoteCluster, // 远程集群名
            remoteClientResponseExecutor, // 远程请求响应使用哪个线程池处理回调
            RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED // 断联后的操作，创建 follower 前必须确认 leader 集群可访问，宁愿重连也不能直接失败
        );

        String leaderIndex = request.getLeaderIndex();
        // 核心：检查 leader 端许可证 + 拉取 leader 索引的元数据 + history UUID
        ccrLicenseChecker.checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
            client,
            remoteCluster,
            leaderIndex,
            listener::onFailure,
            (historyUUID, tuple) -> createFollowerIndex(tuple.v1(), tuple.v2(), request, listener)
        );
    }

    private void createFollowerIndex(
        final IndexMetadata leaderIndexMetadata,
        final DataStream remoteDataStream,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener
    ) {
        if (leaderIndexMetadata == null) { // 索引必须存在
            listener.onFailure(new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not exist"));
            return;
        }
        // 必须开启soft deletes
        if (IndexSettings.INDEX_SOFT_DELETES_SETTING.get(leaderIndexMetadata.getSettings()) == false) {
            listener.onFailure(
                new IllegalArgumentException("leader index [" + request.getLeaderIndex() + "] does not have soft deletes enabled")
            );
            return;
        }
        // 不能是 searchable snapshot，这是只读的远程挂载索引，没有本地完整数据
        if (leaderIndexMetadata.isSearchableSnapshot()) {
            listener.onFailure(
                new IllegalArgumentException(
                    "leader index ["
                        + request.getLeaderIndex()
                        + "] is a searchable snapshot index and cannot be used as a leader index for cross-cluster replication purpose"
                )
            );
            return;
        }

        // 验证用户提供的setting。用户可以传setting来覆盖leader的部分setting配置
        // 用户只能覆盖非 replicated 的 settings，比如 number_of_replicas、refresh_interval 等。
        final Settings replicatedRequestSettings = TransportResumeFollowAction.filter(request.getSettings());
        if (replicatedRequestSettings.isEmpty() == false) {
            // 用户试图覆盖不可修改的 settings → 报错！
            final List<String> unknownKeys = replicatedRequestSettings.keySet()
                .stream()
                .filter(s -> indexScopedSettings.get(s) == null)
                .collect(Collectors.toList());
            final String message;
            if (unknownKeys.isEmpty()) {
                message = String.format(
                    Locale.ROOT,
                    "can not put follower index that could override leader settings %s",
                    replicatedRequestSettings
                );
            } else {
                message = String.format(
                    Locale.ROOT,
                    "unknown setting%s [%s]",
                    unknownKeys.size() == 1 ? "" : "s",
                    String.join(",", unknownKeys)
                );
            }
            listener.onFailure(new IllegalArgumentException(message));
            return;
        }

        // 构造 overrideSettings，覆盖主集群索引的setting
        final Settings overrideSettings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_PROVIDED_NAME, request.getFollowerIndex())
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
            .put(request.getSettings())
            .build();

        // 构造 RestoreSnapshotRequest
        final String leaderClusterRepoName = CcrRepository.NAME_PREFIX + request.getRemoteCluster();
        final RestoreSnapshotRequest restoreRequest = new RestoreSnapshotRequest(
            request.masterNodeTimeout(),
            leaderClusterRepoName,  // repository = "_ccr_leader"
            CcrRepository.LATEST    // snapshot = "_latest_"
        ).indices(request.getLeaderIndex()) // 恢复哪个索引: "orders"
            .indicesOptions(request.indicesOptions())
            .renamePattern("^(.*)$")    // 匹配所有名字
            .renameReplacement(Matcher.quoteReplacement(request.getFollowerIndex()))// 重命名为: "orders-follower"
            .indexSettings(overrideSettings)// 覆盖的 settings
            .quiet(true);   // 不在集群状态中记录 restore 信息
        //这就是"伪装成 Snapshot/Restore"的核心——构造了一个看起来完全正常的 restore 请求，但：
        //  - repository "_ccr_leader" 不是真正的 S3/文件系统仓库，而是 CcrRepository
        //  - snapshot "_latest_" 不是真正的快照 ID，而是特殊标记，意思是"给我 leader 索引的当前最新状态"
        //  - renamePattern/Replacement 把 leader 索引名 orders 改成 follower 索引名 orders-follower

        final Client clientWithHeaders = CcrLicenseChecker.wrapClient(
            this.client,
            threadPool.getThreadContext().getHeaders(),
            clusterService.state()
        );
        // 这里的行为是：
        // 如果收到 失败 → 直接把异常转发给原始 listener（listener.onFailure(e)）
        // 如果收到 成功 → 执行我给的 lambda
        // 之所以失败可以直接转发，是因为失败都是exception类型，而成功是有自己的response类型，需要一层中间的转发
        ActionListener<RestoreService.RestoreCompletionResponse> delegatelistener = listener.delegateFailure(
            (delegatedListener, response) -> afterRestoreStarted(clientWithHeaders, request, delegatedListener, response)
        );

        final BiConsumer<ClusterState, Metadata.Builder> updater;
        if (remoteDataStream == null) {
            // If the index we're following is not part of a data stream, start the
            // restoration of the index normally.
            updater = (clusterState, mdBuilder) -> {};
        } else {
            String followerIndexName = request.getFollowerIndex();
            // This method is used to update the metadata in the same cluster state
            // update as the snapshot is restored.
            updater = (currentState, mdBuilder) -> {
                final String localDataStreamName;

                // If we have been given a data stream name, use that name for the local
                // data stream. See the javadoc for AUTO_FOLLOW_PATTERN_REPLACEMENT
                // for more info.
                final String dsName = request.getDataStreamName();
                if (Strings.hasText(dsName)) {
                    localDataStreamName = dsName;
                } else {
                    // There was no specified name, use the original data stream name.
                    localDataStreamName = remoteDataStream.getName();
                }
                final DataStream localDataStream = mdBuilder.dataStreamMetadata().dataStreams().get(localDataStreamName);
                final Index followerIndex = mdBuilder.get(followerIndexName).getIndex();
                assert followerIndex != null : "expected followerIndex " + followerIndexName + " to exist in the state, but it did not";

                final DataStream updatedDataStream = updateLocalDataStream(
                    followerIndex,
                    localDataStream,
                    localDataStreamName,
                    remoteDataStream
                );
                mdBuilder.put(updatedDataStream);
            };
        }
        threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
            .execute(ActionRunnable.wrap(delegatelistener, l -> restoreService.restoreSnapshot(restoreRequest, l, updater)));
    }

    private void afterRestoreStarted(
        Client clientWithHeaders,
        PutFollowAction.Request request,
        ActionListener<PutFollowAction.Response> originalListener,
        RestoreService.RestoreCompletionResponse response
    ) {
        final ActionListener<PutFollowAction.Response> listener;
        if (ActiveShardCount.NONE.equals(request.waitForActiveShards())) {
            // 默认不等，立即返回
            originalListener.onResponse(new PutFollowAction.Response(true, false, false));
            listener = new ActionListener<>() {

                // listener 替换成一个"只记日志"的空壳
                @Override
                public void onResponse(PutFollowAction.Response response) {
                    logger.debug("put follow {} completed with {}", request, response);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> "put follow " + request + " failed during the restore process", e);
                }
            };
        } else {
            // 用户要求等，成功了才回复用户
            listener = originalListener;
        }

        RestoreClusterStateListener.createAndRegisterListener(
            clusterService,
            response,
            // restore 全部完成后，这个 lambda 会被调用
            listener.delegateFailure((delegatedListener, restoreSnapshotResponse) -> {
                RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
                if (restoreInfo == null) {
                    // 情况1：master 在 restore 过程中挂了，没拿到结果
                    delegatedListener.onResponse(new PutFollowAction.Response(true, false, false));
                } else if (restoreInfo.failedShards() == 0) {
                    // 情况2：所有分片都恢复成功 → 进入最后一步，启动复制
                    initiateFollowing(clientWithHeaders, request, delegatedListener);
                } else {
                    // 情况3：有分片恢复失败了
                    assert restoreInfo.failedShards() > 0 : "Should have failed shards";
                    delegatedListener.onResponse(new PutFollowAction.Response(true, false, false));
                }
            }),
            threadPool.getThreadContext()
        );
    }

    private void initiateFollowing(
        final Client clientWithHeaders,
        final PutFollowAction.Request request,
        final ActionListener<PutFollowAction.Response> listener
    ) {
        assert request.waitForActiveShards() != ActiveShardCount.DEFAULT : "PutFollowAction does not support DEFAULT.";
        FollowParameters parameters = request.getParameters();
        ResumeFollowAction.Request resumeFollowRequest = new ResumeFollowAction.Request(request.masterNodeTimeout());
        resumeFollowRequest.setFollowerIndex(request.getFollowerIndex());
        resumeFollowRequest.setParameters(new FollowParameters(parameters));
        clientWithHeaders.execute(
            ResumeFollowAction.INSTANCE,
            resumeFollowRequest,
            listener.delegateFailureAndWrap(
                (l, r) -> ActiveShardsObserver.waitForActiveShards(
                    clusterService,
                    new String[] { request.getFollowerIndex() },
                    request.waitForActiveShards(),
                    request.ackTimeout(),
                    l.map(result -> new PutFollowAction.Response(true, result, r.isAcknowledged()))
                )
            )
        );
    }

    /**
     * Given the backing index that the follower is going to follow, the local data stream (if it
     * exists) and the remote data stream, return the new local data stream for the local cluster
     * (the follower) updated with whichever information is necessary to restore the new
     * soon-to-be-followed index.
     */
    static DataStream updateLocalDataStream(
        Index backingIndexToFollow,
        DataStream localDataStream,
        String localDataStreamName,
        DataStream remoteDataStream
    ) {
        if (localDataStream == null) {
            // The data stream and the backing indices have been created and validated in the remote cluster,
            // just copying the data stream is in this case safe.
            return remoteDataStream.copy()
                .setName(localDataStreamName)
                .setBackingIndices(
                    // Replicated data streams can't be rolled over, so having the `rolloverOnWrite` flag set to `true` wouldn't make sense
                    // (and potentially even break things).
                    remoteDataStream.getDataComponent().copy().setIndices(List.of(backingIndexToFollow)).setRolloverOnWrite(false).build()
                )
                // Replicated data streams should not have the failure store marked for lazy rollover (which they do by default for lazy
                // failure store creation).
                .setFailureIndices(remoteDataStream.getFailureComponent().copy().setRolloverOnWrite(false).build())
                .setReplicated(true)
                .build();
        } else {
            if (localDataStream.isReplicated() == false) {
                throw new IllegalArgumentException(
                    "cannot follow backing index ["
                        + backingIndexToFollow.getName()
                        + "], because local data stream ["
                        + localDataStream.getName()
                        + "] is no longer marked as replicated"
                );
            }

            final List<Index> backingIndices;
            if (localDataStream.getIndices().contains(backingIndexToFollow) == false) {
                backingIndices = new ArrayList<>(localDataStream.getIndices());
                backingIndices.add(backingIndexToFollow);
                // When following an older backing index it should be positioned before the newer backing indices.
                // Currently the assumption is that the newest index (highest generation) is the write index.
                // (just appending an older backing index to the list of backing indices would break that assumption)
                // Not all backing indices follow the data stream backing indices naming convention (e.g. some might start with
                // "restored-" if they're mounted indices, "shrink-" if they were shrunk, or miscellaneously named indices could be added
                // to the data stream using the modify data stream API) so we use a comparator that partitions the non-standard backing
                // indices at the beginning of the data stream (lower generations) and sorts them amongst themselves, and the rest of the
                // indices (that contain `.ds-`) are sorted based on the original backing index name (ie. ignoring everything up to `.ds-`)
                // The goal is to make sure the prefixed (usually read-only - shrink-, restored-, partial-) backing indices do not end
                // up being the write index of the local data stream.

                String partitionByBackingIndexBaseName = BACKING_INDEX_PREFIX + localDataStream.getName();
                backingIndices.sort(
                    Comparator.comparing((Index o) -> o.getName().contains(partitionByBackingIndexBaseName) ? 1 : -1)
                        .thenComparing((Index o) -> {
                            int backingPrefixPosition = o.getName().indexOf(BACKING_INDEX_PREFIX);
                            return backingPrefixPosition > -1 ? o.getName().substring(backingPrefixPosition) : o.getName();
                        })
                );
            } else {
                // edge case where the index was closed on the follower and was already in the datastream's index list
                backingIndices = localDataStream.getIndices();
            }

            return localDataStream.copy()
                .setBackingIndices(localDataStream.getDataComponent().copy().setIndices(backingIndices).build())
                .setGeneration(remoteDataStream.getGeneration())
                .setMetadata(remoteDataStream.getMetadata())
                .build();
        }
    }

    @Override
    protected ClusterBlockException checkBlock(final PutFollowAction.Request request, final ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getFollowerIndex());
    }
}
