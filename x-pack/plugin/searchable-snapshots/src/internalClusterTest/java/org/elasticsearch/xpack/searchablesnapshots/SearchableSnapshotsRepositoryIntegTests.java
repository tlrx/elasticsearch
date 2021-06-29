/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotRestoreException;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.DELETE_SEARCHABLE_SNAPSHOT_ON_INDEX_DELETION;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class SearchableSnapshotsRepositoryIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    public void testRepositoryUsedBySearchableSnapshotCanBeUpdatedButNotUnregistered() throws Exception {
        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Settings.Builder repositorySettings = randomRepositorySettings();
        createRepository(repositoryName, FsRepository.TYPE, repositorySettings);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(
            indexName,
            Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
        );

        final TotalHits totalHits = internalCluster().client()
            .prepareSearch(indexName)
            .setTrackTotalHits(true)
            .get()
            .getHits()
            .getTotalHits();

        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createSnapshot(repositoryName, snapshotName, List.of(indexName));
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final int nbMountedIndices = 1;
        randomIntBetween(1, 5);
        final String[] mountedIndices = new String[nbMountedIndices];

        for (int i = 0; i < nbMountedIndices; i++) {
            Storage storage = randomFrom(Storage.values());
            String restoredIndexName = (storage == Storage.FULL_COPY ? "fully-mounted-" : "partially-mounted-") + indexName + '-' + i;
            mountSnapshot(repositoryName, snapshotName, indexName, restoredIndexName, Settings.EMPTY, storage);
            assertHitCount(client().prepareSearch(restoredIndexName).setTrackTotalHits(true).get(), totalHits.value);
            mountedIndices[i] = restoredIndexName;
        }

        assertAcked(
            clusterAdmin().preparePutRepository(repositoryName)
                .setType(FsRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(repositorySettings.build())
                        .put(FsRepository.REPOSITORIES_CHUNK_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1L))
                        .build()
                )
        );

        final String updatedRepositoryName;
        if (randomBoolean()) {
            final String snapshotWithMountedIndices = snapshotName + "-with-mounted-indices";
            createSnapshot(repositoryName, snapshotWithMountedIndices, Arrays.asList(mountedIndices));
            assertAcked(client().admin().indices().prepareDelete(mountedIndices));
            assertAcked(clusterAdmin().prepareDeleteRepository(repositoryName));

            updatedRepositoryName = repositoryName + "-with-mounted-indices";
            createRepository(updatedRepositoryName, FsRepository.TYPE, repositorySettings, randomBoolean());

            final RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(
                updatedRepositoryName,
                snapshotWithMountedIndices
            ).setWaitForCompletion(true).setIndices(mountedIndices).get();
            assertEquals(restoreResponse.getRestoreInfo().totalShards(), restoreResponse.getRestoreInfo().successfulShards());
        } else {
            updatedRepositoryName = repositoryName;
        }

        for (int i = 0; i < nbMountedIndices; i++) {
            IllegalStateException exception = expectThrows(
                IllegalStateException.class,
                () -> clusterAdmin().prepareDeleteRepository(updatedRepositoryName).get()
            );
            assertThat(
                exception.getMessage(),
                containsString(
                    "trying to modify or unregister repository ["
                        + updatedRepositoryName
                        + "] that is currently used (found "
                        + (nbMountedIndices - i)
                        + " searchable snapshots indices that use the repository:"
                )
            );
            assertAcked(client().admin().indices().prepareDelete(mountedIndices[i]));
        }

        assertAcked(clusterAdmin().prepareDeleteRepository(updatedRepositoryName));
    }

    public void testMountIndexWithDeletionOfSnapshotFails() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        final Settings.Builder repositorySettings = randomRepositorySettings();
        createRepository(repository, FsRepository.TYPE, repositorySettings);

        final String index = "index-" + suffix;
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String snapshot = "snapshot-" + suffix;
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final boolean deleteSnapshot = randomBoolean();
        final String mounted = mountSnapshot(repository, snapshot, index, deleteSnapshotIndexSettings(deleteSnapshot));

        logger.info("--> index [{}] mounted with [{}={}]", mounted, DELETE_SEARCHABLE_SNAPSHOT_ON_INDEX_DELETION.getKey(), deleteSnapshot);
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);

        final String mountedAgain = randomValueOtherThan(mounted, () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT));

        // the snapshot is already mounted as an index, an attempt to mount the snapshot again
        // with "index.store.snapshot.delete_searchable_snapshot: true" should fail
        {
            SnapshotRestoreException exception = expectThrows(
                SnapshotRestoreException.class,
                () -> mountSnapshot(repository, snapshot, index, mountedAgain, deleteSnapshotIndexSettings(true))
            );
            assertThat(
                exception.getMessage(),
                allOf(
                    containsString("cannot mount snapshot [" + repository + '/'),
                    containsString(snapshot + "] as index [" + mountedAgain + "] with the deletion of snapshot on index removal enabled"),
                    containsString("; another index [" + mounted + '/'),
                    containsString("] uses the snapshot.")
                )
            );
        }

        // the snapshot is already mounted as an index, we can only mount it again
        // with "index.store.snapshot.delete_searchable_snapshot: false"
        {
            if (deleteSnapshot) {
                SnapshotRestoreException exception = expectThrows(
                    SnapshotRestoreException.class,
                    () -> mountSnapshot(repository, snapshot, index, mountedAgain, deleteSnapshotIndexSettings(false))
                );
                assertThat(
                    exception.getMessage(),
                    allOf(
                        containsString("cannot mount snapshot [" + repository + '/'),
                        containsString(snapshot + "] as index [" + mountedAgain + "]; "),
                        containsString("another index [" + mounted + '/'),
                        containsString("] uses the snapshot with the deletion of snapshot on index removal enabled")
                    )
                );

            } else {
                // the snapshot is already mounted as an index, an attempt to mount the snapshot again should succeed if the cascade
                // deletion
                // of the snapshot is not enabled
                mountSnapshot(repository, snapshot, index, mountedAgain, deleteSnapshotIndexSettings(false));
                assertHitCount(client().prepareSearch(mountedAgain).setTrackTotalHits(true).get(), totalHits.value);
            }
        }
    }

    public void testMountIndexWithDeletionOfSnapshotFailsIfNotSingleIndexSnapshot() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        final Settings.Builder repositorySettings = randomRepositorySettings();
        createRepository(repository, FsRepository.TYPE, repositorySettings);

        final int nbIndices = randomIntBetween(1, 5);
        for (int i = 0; i < nbIndices; i++) {
            createAndPopulateIndex("index-" + suffix + '-' + i, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));
        }

        final String snapshot = "snapshot-" + suffix;
        createFullSnapshot(repository, snapshot);
        assertAcked(client().admin().indices().prepareDelete("index-" + suffix + "-*"));

        final String index = "index-" + suffix + '-' + randomInt(nbIndices - 1);
        final String mountedIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        if (nbIndices != 1) {
            SnapshotRestoreException exception = expectThrows(
                SnapshotRestoreException.class,
                () -> mountSnapshot(repository, snapshot, index, mountedIndex, deleteSnapshotIndexSettings(true))
            );
            assertThat(
                exception.getMessage(),
                allOf(
                    containsString("cannot mount snapshot [" + repository + '/'),
                    containsString(snapshot + "] as index [" + mountedIndex + "] with the deletion of snapshot on index removal enabled"),
                    containsString("[index.store.snapshot.delete_searchable_snapshot: true]; "),
                    containsString("snapshot contains [" + nbIndices + "] indices instead of 1.")
                )
            );
        } else {
            mountSnapshot(repository, snapshot, index, mountedIndex, deleteSnapshotIndexSettings(false));
            ensureGreen(mountedIndex);
        }
    }

    private Settings deleteSnapshotIndexSettings(boolean value) {
        return Settings.builder().put(DELETE_SEARCHABLE_SNAPSHOT_ON_INDEX_DELETION.getKey(), value).build();
    }
}
