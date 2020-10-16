/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheIndexService;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.index.store.SearchableSnapshotDirectory.unwrapDirectory;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.isSearchableSnapshotStore;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 0)
public class SearchableSnapshotsFullClusterRestartIT extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected boolean randomCacheSize() {
        return false; // We don't want the cache to be reduced between node restarts
    }

    public void testFullClusterRestart() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();

        final int numDataNodes = randomIntBetween(2, 5);
        internalCluster().startDataOnlyNodes(numDataNodes);
        ensureStableCluster(numDataNodes + 1);

        final String indexName = "test";
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numDataNodes)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        populateIndex(indexName, scaledRandomIntBetween(10, 1_000));

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, FsRepository.TYPE);

        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final CreateSnapshotResponse snapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .get();

        final int numPrimaries = getNumShards(indexName).numPrimaries;
        assertThat(snapshotResponse.getSnapshotInfo().successfulShards(), equalTo(numPrimaries));
        assertThat(snapshotResponse.getSnapshotInfo().failedShards(), equalTo(0));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            indexName,
            repositoryName,
            snapshotName,
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean()).build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse mountResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet();
        assertThat(mountResponse.getRestoreInfo().successfulShards(), equalTo(numPrimaries));
        assertThat(mountResponse.getRestoreInfo().failedShards(), equalTo(0));
        waitForRelocation();
        ensureGreen(indexName);

        waitForAllShardsRecovered(indexName, numPrimaries);

        // Disable allocation and rebalance
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), EnableAllocationDecider.Allocation.NONE)
                        .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
                        .build()
                )
        );

        final List<ShardRouting> shardRoutingsBeforeRestart = client(masterNode).admin()
            .cluster()
            .prepareState()
            .clear()
            .setRoutingTable(true)
            .setIndices(indexName)
            .get()
            .getState()
            .routingTable()
            .allShards(indexName);
        assertThat(shardRoutingsBeforeRestart, hasSize(numPrimaries));

        syncCacheFiles();

        internalCluster().fullRestart();
        ensureStableCluster(numDataNodes + 1);

        // Reenable allocation and rebalance
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .putNull(CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey())
                        .putNull(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey())
                        .build()
                )
        );

        ensureGreen(indexName);
        waitForAllShardsRecovered(indexName, numPrimaries);

        final List<ShardRouting> shardRoutingsAfterRestart = client(masterNode).admin()
            .cluster()
            .prepareState()
            .clear()
            .setRoutingTable(true)
            .setIndices(indexName)
            .get()
            .getState()
            .routingTable()
            .allShards(indexName);
        assertThat(shardRoutingsAfterRestart, hasSize(numPrimaries));

        for (int i = 0; i < numPrimaries; i++) {
            final int shardId = i;
            final ShardRouting before = shardRoutingsBeforeRestart.stream()
                .filter(shardRouting -> shardRouting.shardId().id() == shardId)
                .findFirst()
                .orElseThrow();
            final ShardRouting after = shardRoutingsAfterRestart.stream()
                .filter(shardRouting -> shardRouting.shardId().id() == shardId)
                .findFirst()
                .orElseThrow();
            assertThat(
                "Expecting shard [" + shardId + "] to be allocated to the same node after restart",
                after.currentNodeId(),
                equalTo(before.currentNodeId())
            );
        }

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose(indexName));

            ensureGreen(indexName);
            waitForAllShardsRecovered(indexName, numPrimaries);

            final List<ShardRouting> shardRoutingsAfterClose = client(masterNode).admin()
                .cluster()
                .prepareState()
                .clear()
                .setRoutingTable(true)
                .setIndices(indexName)
                .get()
                .getState()
                .routingTable()
                .allShards(indexName);
            assertThat(shardRoutingsAfterClose, hasSize(numPrimaries));

            for (int i = 0; i < numPrimaries; i++) {
                final int shardId = i;
                final ShardRouting before = shardRoutingsBeforeRestart.stream()
                    .filter(shardRouting -> shardRouting.shardId().id() == shardId)
                    .findFirst()
                    .orElseThrow();
                final ShardRouting after = shardRoutingsAfterClose.stream()
                    .filter(shardRouting -> shardRouting.shardId().id() == shardId)
                    .findFirst()
                    .orElseThrow();
                assertThat(
                    "Expecting shard [" + shardId + "] to be allocated to the same node close",
                    after.currentNodeId(),
                    equalTo(before.currentNodeId())
                );
            }
        }
    }

    private static void waitForAllShardsRecovered(final String indexName, final int numShards) throws Exception {
        assertBusy(() -> {
            final RecoveryResponse recoveries = client().admin().indices().prepareRecoveries(indexName).get();
            assertThat(recoveries.getSuccessfulShards(), equalTo(numShards));
            assertThat(recoveries.getFailedShards(), equalTo(0));
            assertThat(
                recoveries.shardRecoveryStates()
                    .values()
                    .stream()
                    .flatMap(Collection::stream)
                    .filter(recoveryState -> recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT)
                    .filter(recoveryState -> recoveryState.getStage() == RecoveryState.Stage.DONE)
                    .count(),
                equalTo((long) numShards)
            );
        }, 30L, TimeUnit.SECONDS);
    }

    private static void syncCacheFiles() {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                if (isSearchableSnapshotStore(indexService.getIndexSettings().getSettings())) {
                    for (IndexShard indexShard : indexService) {
                        final SearchableSnapshotDirectory directory = unwrapDirectory(indexShard.store().directory());
                        assertThat(directory, notNullValue());
                        directory.syncCacheFiles();
                    }
                }
            }
        }

        for (CacheIndexService cacheIndexService : internalCluster().getDataNodeInstances(CacheIndexService.class)) {
            cacheIndexService.syncCacheFiles();
        }
    }
}
