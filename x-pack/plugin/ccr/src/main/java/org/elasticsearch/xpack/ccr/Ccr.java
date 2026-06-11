/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator;
import org.elasticsearch.xpack.ccr.action.CcrRequests;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowTaskCleaner;
import org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor;
import org.elasticsearch.xpack.ccr.action.TransportActivateAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.action.TransportCcrStatsAction;
import org.elasticsearch.xpack.ccr.action.TransportDeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.action.TransportFollowInfoAction;
import org.elasticsearch.xpack.ccr.action.TransportFollowStatsAction;
import org.elasticsearch.xpack.ccr.action.TransportForgetFollowerAction;
import org.elasticsearch.xpack.ccr.action.TransportGetAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.action.TransportPauseFollowAction;
import org.elasticsearch.xpack.ccr.action.TransportPutAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.action.TransportPutFollowAction;
import org.elasticsearch.xpack.ccr.action.TransportResumeFollowAction;
import org.elasticsearch.xpack.ccr.action.TransportUnfollowAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.repositories.ClearCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.DeleteInternalCcrRepositoryAction;
import org.elasticsearch.xpack.ccr.action.repositories.GetCcrRestoreFileChunkAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutCcrRestoreSessionAction;
import org.elasticsearch.xpack.ccr.action.repositories.PutInternalCcrRepositoryAction;
import org.elasticsearch.xpack.ccr.allocation.CcrPrimaryFollowerAllocationDecider;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;
import org.elasticsearch.xpack.ccr.repository.CcrRestoreSourceService;
import org.elasticsearch.xpack.ccr.rest.RestCcrStatsAction;
import org.elasticsearch.xpack.ccr.rest.RestDeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.rest.RestFollowInfoAction;
import org.elasticsearch.xpack.ccr.rest.RestFollowStatsAction;
import org.elasticsearch.xpack.ccr.rest.RestForgetFollowerAction;
import org.elasticsearch.xpack.ccr.rest.RestGetAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.rest.RestPauseAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.rest.RestPauseFollowAction;
import org.elasticsearch.xpack.ccr.rest.RestPutAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.rest.RestPutFollowAction;
import org.elasticsearch.xpack.ccr.rest.RestResumeAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.rest.RestResumeFollowAction;
import org.elasticsearch.xpack.ccr.rest.RestUnfollowAction;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_FOLLOWING_INDEX_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.CCR_ENABLED_SETTING;

/**
 * Ccr.java 是整个 CCR 功能的"接线板"。它不包含业务逻辑，只负责把各个组件注册到 ES 框架的对应槽位中。
 * 理解这个文件，就知道 CCR 由哪些部件组成、各自承担什么职责。
 * | 接口                 | 能注册什么                                                |
 *   |----------------------|---------------------------------------------------------|
 *   | ActionPlugin         | Transport Action（集群内 RPC）和 REST Handler（HTTP API） |
 *   | PersistentTaskPlugin | 持久化任务执行器                                          |
 *   | EnginePlugin         | 自定义 Lucene 引擎工厂                                    |
 *   | RepositoryPlugin     | 快照仓库实现                                              |
 *   | ClusterPlugin        | 分片分配决策器                                            |
 */
public class Ccr extends Plugin implements ActionPlugin, PersistentTaskPlugin, EnginePlugin, RepositoryPlugin, ClusterPlugin {

    public static final String CCR_THREAD_POOL_NAME = "ccr";
    public static final String CCR_CUSTOM_METADATA_KEY = "ccr";
    public static final String CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS = "leader_index_shard_history_uuids";
    public static final String CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY = "leader_index_uuid";
    public static final String CCR_CUSTOM_METADATA_LEADER_INDEX_NAME_KEY = "leader_index_name";
    public static final String CCR_CUSTOM_METADATA_REMOTE_CLUSTER_NAME_KEY = "remote_cluster_name";

    public static final String REQUESTED_OPS_MISSING_METADATA_KEY = "es.requested_operations_missing";
    public static final TransportVersion TRANSPORT_VERSION_ACTION_WITH_SHARD_ID = TransportVersions.V_8_9_X;

    private final boolean enabled;
    private final Settings settings;
    private final CcrLicenseChecker ccrLicenseChecker;
    private final SetOnce<CcrRestoreSourceService> restoreSourceService = new SetOnce<>();
    private final SetOnce<CcrSettings> ccrSettings = new SetOnce<>();
    private Client client;

    /**
     * Construct an instance of the CCR container with the specified settings.
     *
     * @param settings the settings
     */
    @SuppressWarnings("unused") // constructed reflectively by the plugin infrastructure
    public Ccr(final Settings settings) {
        this(settings, new CcrLicenseChecker(settings));
    }

    /**
     * Construct an instance of the CCR container with the specified settings and license checker.
     *
     * @param settings          the settings
     * @param ccrLicenseChecker the CCR license checker
     */
    Ccr(final Settings settings, final CcrLicenseChecker ccrLicenseChecker) {
        this.settings = settings;
        this.enabled = CCR_ENABLED_SETTING.get(settings);
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    /**
     * 节点启动时创建的长期运行组件
     */
    @Override
    @SuppressWarnings("HiddenField")
    public Collection<?> createComponents(PluginServices services) {
        this.client = services.client();
        if (enabled == false) {
            return emptyList();
        }

        CcrSettings ccrSettings = new CcrSettings(settings, services.clusterService().getClusterSettings());
        this.ccrSettings.set(ccrSettings);
        CcrRestoreSourceService restoreSourceService = new CcrRestoreSourceService(services.threadPool(), ccrSettings);
        this.restoreSourceService.set(restoreSourceService);
        return List.of(
            ccrLicenseChecker,
            restoreSourceService,
            new CcrRepositoryManager(settings, services.clusterService(), client),
            new ShardFollowTaskCleaner(services.clusterService(), services.threadPool(), client),
            new AutoFollowCoordinator(
                settings,
                client,
                services.clusterService(),
                ccrLicenseChecker,
                services.threadPool().relativeTimeInMillisSupplier(),
                services.threadPool()::absoluteTimeInMillis,
                services.threadPool().executor(Ccr.CCR_THREAD_POOL_NAME)
            )
        );
    }

    // PersistentTask 执行器，它负责每个 follower shard 的复制循环
    @Override
    @SuppressWarnings("HiddenField")
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return Collections.singletonList(new ShardFollowTasksExecutor(client, threadPool, clusterService, settingsModule));
    }

    /**
     * 注册各种action
     */
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction = new ActionHandler<>(XPackUsageFeatureAction.CCR, CCRUsageTransportAction.class);
        var infoAction = new ActionHandler<>(XPackInfoFeatureAction.CCR, CCRInfoTransportAction.class);
        if (enabled == false) {
            return Arrays.asList(usageAction, infoAction);
        }

        return Arrays.asList(
            // internal actions - 内部数据传输，不暴露给用户
            // 将从Leader拉取的操作批量写入Follower shard（Follower本地执行）
            new ActionHandler<>(BulkShardOperationsAction.INSTANCE, TransportBulkShardOperationsAction.class),
            // 从Leader shard拉取增量操作（Follower → Leader）
            new ActionHandler<>(ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class),
            // 创建内部CCR Repository（Follower本地执行）
            new ActionHandler<>(
                PutInternalCcrRepositoryAction.INSTANCE,
                PutInternalCcrRepositoryAction.TransportPutInternalRepositoryAction.class
            ),
            // 删除内部CCR Repository（Follower本地执行）
            new ActionHandler<>(
                DeleteInternalCcrRepositoryAction.INSTANCE,
                DeleteInternalCcrRepositoryAction.TransportDeleteInternalRepositoryAction.class
            ),
            // Bootstrap时在Leader建立文件恢复会话（Follower → Leader）
            new ActionHandler<>(PutCcrRestoreSessionAction.INTERNAL_INSTANCE, PutCcrRestoreSessionAction.InternalTransportAction.class),
            new ActionHandler<>(PutCcrRestoreSessionAction.INSTANCE, PutCcrRestoreSessionAction.TransportAction.class),
            // Bootstrap时关闭Leader侧的文件恢复会话（Follower → Leader）
            new ActionHandler<>(ClearCcrRestoreSessionAction.INTERNAL_INSTANCE, ClearCcrRestoreSessionAction.InternalTransportAction.class),
            new ActionHandler<>(ClearCcrRestoreSessionAction.INSTANCE, ClearCcrRestoreSessionAction.TransportAction.class),
            // Bootstrap时从Leader拉取Lucene文件分块（Follower → Leader）
            new ActionHandler<>(GetCcrRestoreFileChunkAction.INTERNAL_INSTANCE, GetCcrRestoreFileChunkAction.InternalTransportAction.class),
            new ActionHandler<>(GetCcrRestoreFileChunkAction.INSTANCE, GetCcrRestoreFileChunkAction.TransportAction.class),
            // stats action - 统计信息查询
            // 查询单个follow索引各shard的实时复制统计
            new ActionHandler<>(FollowStatsAction.INSTANCE, TransportFollowStatsAction.class),
            // 查询CCR整体统计（含auto-follow统计）
            new ActionHandler<>(CcrStatsAction.INSTANCE, TransportCcrStatsAction.class),
            // 查询follow索引的配置参数
            new ActionHandler<>(FollowInfoAction.INSTANCE, TransportFollowInfoAction.class),
            // follow actions - Follow生命周期管理
            // 创建follower索引并启动复制（Bootstrap + 启动persistent task）
            new ActionHandler<>(PutFollowAction.INSTANCE, TransportPutFollowAction.class),
            // 恢复已暂停的follow（重新启动persistent task）
            new ActionHandler<>(ResumeFollowAction.INSTANCE, TransportResumeFollowAction.class),
            // 暂停follow（停止persistent task，保留follower索引）
            new ActionHandler<>(PauseFollowAction.INSTANCE, TransportPauseFollowAction.class),
            // 彻底解除follow关系（follower变为普通可写索引）
            new ActionHandler<>(UnfollowAction.INSTANCE, TransportUnfollowAction.class),
            // auto-follow actions - 自动Follow规则管理
            // 删除auto-follow pattern
            new ActionHandler<>(DeleteAutoFollowPatternAction.INSTANCE, TransportDeleteAutoFollowPatternAction.class),
            // 创建auto-follow pattern（匹配Leader新索引时自动follow）
            new ActionHandler<>(PutAutoFollowPatternAction.INSTANCE, TransportPutAutoFollowPatternAction.class),
            // 查询auto-follow pattern
            new ActionHandler<>(GetAutoFollowPatternAction.INSTANCE, TransportGetAutoFollowPatternAction.class),
            // 激活/停用auto-follow pattern
            new ActionHandler<>(ActivateAutoFollowPatternAction.INSTANCE, TransportActivateAutoFollowPatternAction.class),
            // forget follower action - Leader侧主动清理follower的retention lease
            new ActionHandler<>(ForgetFollowerAction.INSTANCE, TransportForgetFollowerAction.class),
            usageAction,
            infoAction
        );
    }

    // 每个 REST handler 对应一个 Transport Action，就是 HTTP 入口到内部 RPC 的映射
    public List<RestHandler> getRestHandlers(
        Settings unused,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
            // stats API
            new RestFollowStatsAction(),
            new RestCcrStatsAction(),
            new RestFollowInfoAction(),
            // follow APIs
            new RestPutFollowAction(),
            new RestResumeFollowAction(),
            new RestPauseFollowAction(),
            new RestUnfollowAction(),
            // auto-follow APIs
            new RestDeleteAutoFollowPatternAction(),
            new RestPutAutoFollowPatternAction(),
            new RestGetAutoFollowPatternAction(),
            new RestPauseAutoFollowPatternAction(),
            new RestResumeAutoFollowPatternAction(),
            // forget follower API
            new RestForgetFollowerAction()
        );
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            // Persistent action requests
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, ShardFollowTask.NAME, ShardFollowTask::readFrom),

            // Task statuses
            new NamedWriteableRegistry.Entry(
                Task.Status.class,
                ShardFollowNodeTaskStatus.STATUS_PARSER_NAME,
                ShardFollowNodeTaskStatus::new
            ),

            // usage api
            new NamedWriteableRegistry.Entry(XPackFeatureUsage.class, XPackField.CCR, CCRInfoTransportAction.Usage::new)
        );
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
            // auto-follow metadata, persisted into the cluster state as XContent
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(AutoFollowMetadata.TYPE),
                AutoFollowMetadata::fromXContent
            ),
            // persistent action requests
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(ShardFollowTask.NAME),
                ShardFollowTask::fromXContent
            ),
            // task statuses
            new NamedXContentRegistry.Entry(
                ShardFollowNodeTaskStatus.class,
                new ParseField(ShardFollowNodeTaskStatus.STATUS_PARSER_NAME),
                ShardFollowNodeTaskStatus::fromXContent
            )
        );
    }

    /**
     * The settings defined by CCR.
     *
     * @return the settings
     */
    public List<Setting<?>> getSettings() {
        return CcrSettings.getSettings();
    }

    /**
     * The optional engine factory for CCR. This method inspects the index settings for the {@link CcrSettings#CCR_FOLLOWING_INDEX_SETTING}
     * setting to determine whether or not the engine implementation should be a following engine.
     * ES 打开每个 shard 时会问所有 EnginePlugin："这个索引你要用自定义引擎吗？
     * 如果 index.xpack.ccr.following_index = true，就用 FollowingEngine（接受 leader 的 seq_no），否则用默认的 InternalEngine。
     * @return the optional engine factory
     */
    public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
        if (CCR_FOLLOWING_INDEX_SETTING.get(indexSettings.getSettings())) {
            return Optional.of(new FollowingEngineFactory());
        } else {
            return Optional.empty();
        }
    }

    /**
     * CCR 的所有后台工作（拉取操作、写入操作、auto-follow 扫描）都跑在这个专用线程池里，不会抢占 ES 其他线程池的资源
     */
    @SuppressWarnings("HiddenField")
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(
            new FixedExecutorBuilder(
                settings,
                CCR_THREAD_POOL_NAME,
                32,
                100,
                "xpack.ccr.ccr_thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
    }

    /**
     * - 注册为内部仓库（用户不可见，不出现在 GET _snapshot 中）
     * - 类型名 "_ccr_"
     * - 用于 Bootstrap 阶段：follower 创建时走 snapshot/restore 流程，但实际数据是从 leader 实时拉取的
     */
    @Override
    public Map<String, Repository.Factory> getInternalRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        Repository.Factory repositoryFactory = (metadata) -> new CcrRepository(
            metadata,
            client,
            settings,
            ccrSettings.get(),
            clusterService.getClusterApplierService().threadPool()
        );
        return Collections.singletonMap(CcrRepository.TYPE, repositoryFactory);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (enabled) {
            indexModule.addIndexEventListener(this.restoreSourceService.get());
        }
    }

    /**
     * 禁止用户手动修改 follower 索引的 mapping
     */
    @Override
    public Collection<RequestValidators.RequestValidator<PutMappingRequest>> mappingRequestValidators() {
        return Collections.singletonList(CcrRequests.CCR_PUT_MAPPING_REQUEST_VALIDATOR);
    }

    /**
     * 禁止用户手动修改 follower 索引的 aliases
     */
    @Override
    public Collection<RequestValidators.RequestValidator<IndicesAliasesRequest>> indicesAliasesRequestValidators() {
        return Collections.singletonList(CcrRequests.CCR_INDICES_ALIASES_REQUEST_VALIDATOR);
    }

    /**
     * 确保 follower 索引的 primary shard 在 bootstrap 期间只能分配到具有 remote_cluster_client 角色的节点上
     * 否则无法连接 leader 拉取数据
     */
    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings unused, ClusterSettings clusterSettings) {
        return List.of(new CcrPrimaryFollowerAllocationDecider());
    }
}
