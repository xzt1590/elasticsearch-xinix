/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.xpack.ccr.CcrSettings;

/**
 * An allocation decider that ensures primary shards of follower indices that are being bootstrapped are assigned to nodes that have the
 * remote cluster client role. This is necessary as those nodes reach out to the leader shards on the remote cluster to copy Lucene segment
 * files and periodically renew retention leases during the bootstrap.
 * 但 ES 集群中不是所有节点都配置了远程集群连接能力。只有设置了 remote_cluster_client 角色的节点才能连接远程集群。
 * 如果一个分片被分配到了没有 remote_cluster_client 角色的节点上，restoreShard() 执行时就连不上 Leader，直接失败。
 * 所以需要一个分片分配决策器，在分配阶段就拦住：follower 的主分片（正在 bootstrap 的那些）只能分配到有 remote_cluster_client 角色的节点。
 */
public final class CcrPrimaryFollowerAllocationDecider extends AllocationDecider {
    static final String NAME = "ccr_primary_follower";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final IndexMetadata indexMetadata = allocation.metadata().index(shardRouting.index());
        // 这个索引是 CCR follower 吗？不是就不关 ccr 的事
        if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(indexMetadata.getSettings()) == false) {
            return allocation.decision(Decision.YES, NAME, "shard is not a follower and is not under the purview of this decider");
        }
        // 这个分片是主分片吗？不是就直接放行，因为副本不需要跨集群同步
        if (shardRouting.primary() == false) {
            return allocation.decision(Decision.YES, NAME, "shard is a replica follower and is not under the purview of this decider");
        }
        final RecoverySource recoverySource = shardRouting.recoverySource();
        // 这个分片还在bootstrap阶段吗？不是就直接放行，因为分片在bootstrap阶段已经分配好了
        if (recoverySource == null || recoverySource.getType() != RecoverySource.Type.SNAPSHOT) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "shard is a primary follower but was bootstrapped already; hence is not under the purview of this decider"
            );
        }
        // 目标节点有remote_cluster_client角色吗？没有就拒绝分配到这个节点
        if (node.node().isRemoteClusterClient() == false) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "shard is a primary follower and being bootstrapped, but node does not have the "
                    + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName()
                    + " role"
            );
        }
        return allocation.decision(
            Decision.YES,
            NAME,
            "shard is a primary follower and node has the " + DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName() + " role"
        );
    }
}
