/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.snapshots.SnapshotState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.DELETE_SEARCHABLE_SNAPSHOT_ON_INDEX_DELETION;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

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
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

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

                final ActionFuture<?> future = waitForSnapshotDeletion(repository, snapshot);
                assertAcked(client().admin().indices().prepareDelete(mounted));
                future.get();

            } else {
                // the snapshot is already mounted as an index, an attempt to mount the snapshot again should succeed if the cascade
                // deletion of the snapshot is not enabled
                mountSnapshot(repository, snapshot, index, mountedAgain, deleteSnapshotIndexSettings(false));
                assertHitCount(client().prepareSearch(mountedAgain).setTrackTotalHits(true).get(), totalHits.value);
                assertAcked(client().admin().indices().prepareDelete(mountedAgain));
                assertAcked(client().admin().indices().prepareDelete(mounted));
                assertAcked(client().admin().cluster().prepareDeleteSnapshot(repository, snapshot).get());
            }
        }
    }

    public void testMountIndexWithDeletionOfSnapshotFailsIfNotSingleIndexSnapshot() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final int nbIndices = randomIntBetween(1, 5);
        for (int i = 0; i < nbIndices; i++) {
            createAndPopulateIndex(
                "index-" + suffix + '-' + i,
                Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
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

    public void testSnapshotAlreadyMarkedAsDeletedCannotBeRestoredOrClonedOrMounted() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index-" + suffix;
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final TotalHits totalHits = internalCluster().client().prepareSearch(index).setTrackTotalHits(true).get().getHits().getTotalHits();

        final String snapshot = "snapshot-" + suffix;
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = mountSnapshot(repository, snapshot, index, deleteSnapshotIndexSettings(true));
        assertHitCount(client().prepareSearch(mounted).setTrackTotalHits(true).get(), totalHits.value);

        final ActionFuture<?> future = waitForSnapshotDeletion(repository, snapshot);
        assertAcked(client().admin().indices().prepareDelete(mounted));

        ConcurrentSnapshotExecutionException exception = null;
        switch (randomInt(2)) {
            case 0:
                final String restoredIndex = randomValueOtherThan(mounted, () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
                logger.info("--> restoring snapshot as index [{}]", restoredIndex);
                exception = expectThrows(ConcurrentSnapshotExecutionException.class, () -> {
                    final RestoreSnapshotResponse restoreResponse = client().admin()
                        .cluster()
                        .prepareRestoreSnapshot(repository, snapshot)
                        .setIndices(index)
                        .setWaitForCompletion(true)
                        .get();
                    assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(getNumShards(restoredIndex).numPrimaries));
                    assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));
                });
                assertThat(exception.getMessage(),
                    anyOf(
                        containsString("cannot restore a snapshot while a snapshot deletion is in-progress"),
                        containsString("cannot restore a snapshot already marked as deleted")
                    )
                );
                break;

            case 1:
                final String mountedIndex = randomValueOtherThan(mounted, () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
                logger.info("--> mounting snapshot as index [{}]", mountedIndex);
                exception = expectThrows(
                    ConcurrentSnapshotExecutionException.class,
                    () -> mountSnapshot(repository, snapshot, index, mountedIndex, deleteSnapshotIndexSettings(randomBoolean()))
                );
                assertThat(exception.getMessage(),
                    anyOf(
                        containsString("cannot restore a snapshot while a snapshot deletion is in-progress"),
                        containsString("cannot restore a snapshot already marked as deleted")
                    )
                );
                break;

            case 2:
                final String clone = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                logger.info("--> cloning snapshot as [{}]", clone);
                exception = expectThrows(
                    ConcurrentSnapshotExecutionException.class,
                    () -> client().admin().cluster().prepareCloneSnapshot(repository, snapshot, clone).setIndices(index).get()
                );
                assertThat(exception.getMessage(),
                    anyOf(
                        containsString("cannot clone from snapshot that is being deleted"),
                        containsString("cannot restore a snapshot already marked as deleted")
                    )
                );
                break;

            default:
                throw new AssertionError();
        }
        future.get();
        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareDeleteSnapshot(repository, snapshot).get());
    }

    public void testDeleteSnapshotThatIsMountedAsIndexBwc() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index-" + suffix;
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final String snapshot = "snapshot-" + suffix;
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = "mounted-" + suffix;

        mountSnapshot(repository, snapshot, index, mounted, Settings.EMPTY, randomFrom(Storage.values()));
        assertAcked(client().admin().cluster().prepareDeleteSnapshot(repository, snapshot).get());
        assertAcked(client().admin().indices().prepareDelete(mounted));
    }

    public void testSearchableSnapshotIsDeletedAfternMountedIndexIsDeleted() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(repository, FsRepository.TYPE, randomRepositorySettings());

        final String index = "index-" + suffix;
        createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

        final String snapshot = "snapshot-" + suffix;
        createSnapshot(repository, snapshot, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));

        final String mounted = "mounted-" + suffix;
        mountSnapshot(repository, snapshot, index, mounted, deleteSnapshotIndexSettings(true), randomFrom(Storage.values()));
        SnapshotException exception = expectThrows(
            SnapshotException.class,
            () -> client().admin().cluster().prepareDeleteSnapshot(repository, snapshot).get()
        );
        assertThat(
            exception.getMessage(),
            allOf(
                containsString("cannot delete snapshot [" + snapshot + '/'),
                containsString("] used by searchable snapshots index [" + mounted + '/')
            )
        );

        final ActionFuture<?> future = waitForSnapshotDeletion(repository, snapshot);
        assertAcked(client().admin().indices().prepareDelete(mounted));
        future.get();

        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareDeleteSnapshot(repository, snapshot).get());
    }

    public void testSearchableSnapshotsAreDeletedAfterMountedIndicesAreDeleted() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);
        final String repository = "repository-" + suffix;
        createRepository(logger, repository, "mock");

        final int nbIndices = randomIntBetween(2, 10);
        final String[] mountedIndices = new String[nbIndices];

        for (int i = 0; i < nbIndices; i++) {
            final String index = "index-" + i;
            createAndPopulateIndex(index, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true));

            final String snapshot = "snapshot-" + i;
            createSnapshot(repository, snapshot, List.of(index));

            final String mounted = "mounted-" + index;
            mountSnapshot(repository, snapshot, index, mounted, deleteSnapshotIndexSettings(true), randomFrom(Storage.values()));
            mountedIndices[i] = mounted;
        }
        blockAllDataNodes(repository);

        final List<ActionFuture<?>> futures = new ArrayList<>();
        {
            for (int i = 0; i < nbIndices; i++) {
                if (randomBoolean()) {
                    final ActionFuture<?> future;
                    switch (randomInt(2)) {
                        case 0:
                            future = client().admin().cluster().prepareCreateSnapshot(repository, "other-" + i)
                                .setIndices("index-" + i).setWaitForCompletion(true).execute();
                            break;
                        case 1:
                            future = client().admin().cluster().prepareRestoreSnapshot(repository, "snapshot-" + i)
                                .setIndices("index-" + i).setRenamePattern("(.+)").setRenameReplacement("$1-restored-" + i)
                                .setWaitForCompletion(true).execute();
                            break;
                        case 2:
                            future = client().admin().cluster().prepareCloneSnapshot(repository, "snapshot-" + i, "clone-" + i)
                                .setIndices("index-" + i).execute();
                            break;
                        default:
                            throw new AssertionError();
                    }
                    futures.add(future);
                }
            }
            if (futures.isEmpty() == false) {
                awaitClusterState(state ->
                    state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().size() > 0
                        || state.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY).isEmpty() == false);
            }
        }
        {
            for (int i = 0; i < nbIndices; i++) {
                futures.add(waitForSnapshotDeletion(repository, "snapshot-" + i));
            }

            final List<String> remainingIndicesToDelete = new ArrayList<>(asSet(mountedIndices));
            while (remainingIndicesToDelete.isEmpty() == false) {
                List<String> toDelete = randomValueOtherThanMany(List::isEmpty, () -> randomSubsetOf(remainingIndicesToDelete));
                futures.add(client().admin().indices().prepareDelete(toDelete.toArray(String[]::new)).execute());
                toDelete.forEach(remainingIndicesToDelete::remove);
            }
        }

        unblockAllDataNodes(repository);

        assertBusy(() -> {
            for (ActionFuture<?> operation : futures) {
                assertTrue(operation.isDone());
                try {
                    Object response = operation.get();
                    if (response instanceof AcknowledgedResponse) {
                        assertAcked((AcknowledgedResponse) response);
                    } else if (response instanceof CreateSnapshotResponse) {
                        final SnapshotInfo snapshotInfo = ((CreateSnapshotResponse) response).getSnapshotInfo();
                        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
                        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
                    } else if (response instanceof RestoreSnapshotResponse) {
                        final RestoreSnapshotResponse restoreResponse = ((RestoreSnapshotResponse) response);
                        assertThat(restoreResponse.getRestoreInfo().successfulShards(), greaterThanOrEqualTo(1));
                        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));
                    } else {
                        throw new AssertionError("Unknown response type: " + response);
                    }
                } catch (ExecutionException e) {
                    final Throwable csee = ExceptionsHelper.unwrap(e, ConcurrentSnapshotExecutionException.class);
                    assertThat(csee, instanceOf(ConcurrentSnapshotExecutionException.class));
                    assertThat(csee.getMessage(),
                        anyOf(
                            containsString("cannot clone a snapshot that is marked as deleted"),
                            containsString("cannot restore a snapshot already marked as deleted")
                    ));
                }
            }
        });

        GetSnapshotsResponse response = clusterAdmin().prepareGetSnapshots(repository).get();
        for (SnapshotInfo snapshot : response.getSnapshots()) {
            assertFalse(snapshot.snapshotId().getName().startsWith("snapshot-"));
        }
    }

    private Settings deleteSnapshotIndexSettings(boolean value) {
        return Settings.builder().put(DELETE_SEARCHABLE_SNAPSHOT_ON_INDEX_DELETION.getKey(), value).build();
    }

    /**
     * Waits for the given snapshot to be marked as "to delete" in the given repository metadata; then waits for the snapshot to be deleted.
     */
    private ActionFuture<AcknowledgedResponse> waitForSnapshotDeletion(final String repository, final String snapshot) {
        final PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.addListener(new ClusterStateListener() {

            boolean marked = false;

            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final RepositoryMetadata repositoryMetadata = event.state()
                    .metadata()
                    .custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY)
                    .repository(repository);
                assert repositoryMetadata != null;

                if (marked == false) {
                    for (SnapshotId snapshotToDelete : repositoryMetadata.snapshotsToDelete()) {
                        if (snapshot.equals(snapshotToDelete.getName())) {
                            logger.info("--> snapshot {} found in list of snapshots to delete in repository metadata", snapshotToDelete);
                            marked = true;
                            return;
                        }
                    }
                } else if (repositoryMetadata.hasSnapshotsToDelete() == false) {
                    final SnapshotDeletionsInProgress deletionsInProgress = event.state().custom(SnapshotDeletionsInProgress.TYPE);
                    if (deletionsInProgress == null || deletionsInProgress.hasDeletionsInProgress() == false) {
                        logger.info("--> snapshot {} removed from list of snapshots to delete in repository metadata", snapshot);
                        clusterService.removeListener(this);
                        future.onResponse(AcknowledgedResponse.of(true));
                    }
                }
            }
        });
        return future;
    }
}
