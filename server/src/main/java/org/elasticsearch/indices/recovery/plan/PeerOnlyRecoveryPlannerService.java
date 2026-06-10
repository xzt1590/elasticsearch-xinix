/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery.plan;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.List;

import static org.elasticsearch.common.util.CollectionUtils.concatLists;

/**
 * Service in charge of computing a {@link ShardRecoveryPlan} using only the physical files from the source peer.
 *   对比主分片和副本的文件列表，分成三类：
 *
 *   | 分类      | 含义                       | 处理方式           |
 *   |-----------|----------------------------|--------------------|
 *   | identical | 两边都有，且 checksum 相同 | 不传，副本直接复用 |
 *   | different | 两边都有，但 checksum 不同 | 需要传             |
 *   | missing   | 主分片有，副本没有         | 需要传             |
 *
 *   举例：
 *   主分片文件:  _0.cfs(checksum=aaa), _1.cfs(checksum=bbb), _2.cfs(checksum=ccc)
 *   副本文件:    _0.cfs(checksum=aaa), _1.cfs(checksum=xxx)
 *
 *   recoveryDiff:
 *     identical = [_0.cfs]       → 不传
 *     different = [_1.cfs]       → 要传（checksum 不同，说明内容不同）
 *     missing   = [_2.cfs]       → 要传
 *
 *   最终需要传输: _1.cfs + _2.cfs
 *
 *   这就是为什么节点短暂重启后全量恢复也不一定很慢——大部分 segment 文件没变，只需要传增量的。
 */
public class PeerOnlyRecoveryPlannerService implements RecoveryPlannerService {
    public static final RecoveryPlannerService INSTANCE = new PeerOnlyRecoveryPlannerService();

    @Override
    public void computeRecoveryPlan(
        ShardId shardId,
        @Nullable String shardStateIdentifier,
        Store.MetadataSnapshot sourceMetadata,
        Store.MetadataSnapshot targetMetadata,
        long startingSeqNo,
        int translogOps,
        IndexVersion targetVersion,
        boolean useSnapshots,
        boolean primaryRelocation,
        ActionListener<ShardRecoveryPlan> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            Store.RecoveryDiff recoveryDiff = sourceMetadata.recoveryDiff(targetMetadata);
            List<StoreFileMetadata> filesMissingInTarget = concatLists(recoveryDiff.missing, recoveryDiff.different);
            return new ShardRecoveryPlan(
                ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY,
                filesMissingInTarget,
                recoveryDiff.identical,
                startingSeqNo,
                translogOps,
                sourceMetadata
            );
        });
    }
}
