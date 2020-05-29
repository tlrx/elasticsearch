/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.action.RepositoryStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.RepositoryStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.RepositoryStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsResponse;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SearchableSnapshotsLazyRecoveryTests extends BaseSearchableSnapshotsIntegTestCase {

    public void testLazyRecovery() throws Exception {
        final String repository = "_repository";
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(repository)
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );

        final String index = "index";
        // Peer recovery always copies .liv files but we do not permit writing to searchable snapshot directories so this doesn't work, but
        // we can bypass this by forcing soft deletes to be used. TODO this restriction can be lifted when #55142 is resolved.
        assertAcked(prepareCreate(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = between(10, 10_000); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(index).setSource("timestamp", System.nanoTime()));
        }
        indexRandom(true, true, indexRequestBuilders);
        assertThat(client().admin().indices().prepareForceMerge(index).get().getFailedShards(), equalTo(0));

        final String snapshot = "snapshot";
        final CreateSnapshotResponse createSnapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(repository, snapshot)
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshot.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshot.getSnapshotInfo().successfulShards(), equalTo(createSnapshot.getSnapshotInfo().totalShards()));

        assertAcked(client().admin().indices().prepareDelete(index));

        final String mountedIndex = "mounted";
        final MountSearchableSnapshotRequest mountSnapshot = new MountSearchableSnapshotRequest(
            mountedIndex,
            repository,
            createSnapshot.getSnapshotInfo().snapshotId().getName(),
            index,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), randomBoolean())
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse restoreSnapshot = client().execute(MountSearchableSnapshotAction.INSTANCE, mountSnapshot).get();
        assertThat(restoreSnapshot.getRestoreInfo().failedShards(), equalTo(0));

        ensureGreen(mountedIndex);

        {
            final SearchableSnapshotsStatsResponse snapshotsStatsResponse = client().execute(
                SearchableSnapshotsStatsAction.INSTANCE,
                new SearchableSnapshotsStatsRequest(mountedIndex)
            ).get();
            for (SearchableSnapshotShardStats snapshotStats : snapshotsStatsResponse.getStats()) {
                assertThat(snapshotStats.getShardRouting().state(), equalTo(ShardRoutingState.STARTED));
                assertThat(snapshotStats.getStats(), empty());
            }
        }
        {
            final RepositoryStatsResponse repositoryStatsResponse = client().execute(
                RepositoryStatsAction.INSTANCE,
                new RepositoryStatsRequest(repository)
            ).get();
            assertThat(repositoryStatsResponse.getStats().requestCounts.entrySet(), empty());
        }

        internalCluster().fullRestart();
        ensureGreen(mountedIndex);

        {
            final SearchableSnapshotsStatsResponse snapshotsStatsResponse = client().execute(
                SearchableSnapshotsStatsAction.INSTANCE,
                new SearchableSnapshotsStatsRequest(mountedIndex)
            ).get();
            for (SearchableSnapshotShardStats snapshotStats : snapshotsStatsResponse.getStats()) {
                assertThat(snapshotStats.getShardRouting().state(), equalTo(ShardRoutingState.STARTED));
                assertThat(snapshotStats.getStats(), empty());
            }
        }
        {
            final RepositoryStatsResponse repositoryStatsResponse = client().execute(
                RepositoryStatsAction.INSTANCE,
                new RepositoryStatsRequest(repository)
            ).get();
            assertThat(repositoryStatsResponse.getStats().requestCounts.entrySet(), empty());
        }
    }
}
