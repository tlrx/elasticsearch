/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

public class SearchableSnapshotsDeletions implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotsDeletions.class);

    private final Map<String, RepositoryDeletions> repositories = new HashMap<>();
    private final ThreadPool threadPool;
    private final Client client;

    public SearchableSnapshotsDeletions(ThreadPool threadPool, Client client) {
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            final RepositoriesMetadata repositoriesMetadata = event.state().metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);

            final Set<String> onGoingRepositoryDeletions = new HashSet<>(repositories.keySet());
            for (String name : onGoingRepositoryDeletions) {
                if (repositoriesMetadata.repository(name) == null) {
                    repositories.remove(name);
                }
            }

            for (RepositoryMetadata repositoryMetadata : repositoriesMetadata.repositories()) {
                final String name = repositoryMetadata.name();
                if (repositoryMetadata.hasSnapshotsToDelete() == false) {
                    repositories.remove(name);
                    continue;
                }

                boolean schedule = false;
                RepositoryDeletions repository = repositories.get(name);
                if (repository == null) {
                    repository = new RepositoryDeletions(name);
                    repositories.put(name, repository);
                    schedule = true;
                }
                repository.addSnapshotsToDelete(repositoryMetadata.snapshotsToDelete());

                if (schedule) {
                    processRepositoryDeletions(repository);
                }
            }
        }
    }

    private void processRepositoryDeletions(final RepositoryDeletions repository) {
        final SnapshotId nextDeletion = repository.nextDeletion();
        if (nextDeletion != null) {
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    logger.trace("[{}] triggering deletion of searchable snapshot [{}]", repository.name, nextDeletion);
                    final DeleteSnapshotRequest request = new DeleteSnapshotRequest(repository.name, nextDeletion.getName());
                    client.execute(DeleteSnapshotAction.INSTANCE, request,
                        ActionListener.runAfter(new ActionListener<>() {
                            @Override
                            public void onResponse(AcknowledgedResponse response) {
                                if (response.isAcknowledged()) {
                                    repository.onDeletionAcked(nextDeletion);
                                } else {
                                    repository.onDeletionFailure(nextDeletion, null);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (e instanceof SnapshotMissingException) {
                                    repository.onDeletionAcked(nextDeletion);
                                } else {
                                    repository.onDeletionFailure(nextDeletion, e);
                                }
                            }
                        }, () -> processRepositoryDeletions(repository)));
                }

                @Override
                public void onFailure(Exception e) {
                    repository.onDeletionFailure(nextDeletion, e);
                }
            });
        }
    }

    private static class RepositoryDeletions {

        private final Set<SnapshotId> knownSnapshotIds = ConcurrentCollections.newConcurrentSet();
        private final Queue<SnapshotId> queue = ConcurrentCollections.newQueue();
        private final String name;

        private RepositoryDeletions(String name) {
            this.name = Objects.requireNonNull(name);
        }

        private void addSnapshotsToDelete(List<SnapshotId> snapshotsToDelete) {
            for (SnapshotId snapshotToDelete : snapshotsToDelete) {
                if (knownSnapshotIds.add(snapshotToDelete)) {
                    queue.add(snapshotToDelete);
                }
            }
        }

        private SnapshotId nextDeletion() {
            return queue.poll();
        }

        private void onDeletionAcked(SnapshotId snapshotId) {
            logger.debug("[{}] searchable snapshot deletion [{}] succeed", name, snapshotId);
            final boolean removed = knownSnapshotIds.remove(snapshotId);
            assert removed : snapshotId;
        }

        private void onDeletionFailure(SnapshotId snapshotId, @Nullable Exception e) {
            if (e == null) {
                logger.debug("[{}] failed to delete searchable snapshot [{}], retrying (response not acknowledged)", name, snapshotId);
                assert knownSnapshotIds.contains(snapshotId);
                queue.add(snapshotId);

            } else if (e instanceof ConcurrentSnapshotExecutionException) {
                logger.debug("[{}] failed to delete searchable snapshot [{}], retrying (concurrent operation running)", name, snapshotId);
                assert knownSnapshotIds.contains(snapshotId);
                queue.add(snapshotId);

            } else {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to delete searchable snapshot [{}]", name, snapshotId), e);
                final boolean removed = knownSnapshotIds.remove(snapshotId);
                assert removed : snapshotId;
            }
        }
    }
}
