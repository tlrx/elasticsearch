/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

public class SearchableSnapshotsDeleter implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotsDeleter.class);

    private final Map<String, AsyncSnapshotsDeletions> onGoingDeletions = new HashMap<>();
    private final ThreadPool threadPool;
    private final Client client;

    public SearchableSnapshotsDeleter(ThreadPool threadPool, Client client) {
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            RepositoriesMetadata repositories = event.state().metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            for (RepositoryMetadata repository : repositories.repositories()) {
                if (repository.hasSnapshotsToDelete() == false) {
                    onGoingDeletions.remove(repository.name());
                    continue;
                }
                if (onGoingDeletions.containsKey(repository.name()) == false) {
                    threadPool.generic().execute(onGoingDeletions.computeIfAbsent(repository.name(), name ->
                        new AsyncSnapshotsDeletions(repository.name(), repository.snapshotsToDelete())
                    ));
                }
            }
        }
    }

    private class AsyncSnapshotsDeletions extends AbstractRunnable {

        private final LinkedBlockingQueue<SnapshotId> queue = new LinkedBlockingQueue<>();
        private final String repository;

        public AsyncSnapshotsDeletions(String repository, List<SnapshotId> snapshotsToDelete) {
            this.repository = Objects.requireNonNull(repository);
            queue.addAll(snapshotsToDelete);
        }

        @Override
        protected void doRun() throws Exception {
            SnapshotId snapshot = null;
            while ((snapshot = queue.poll()) != null) {
                boolean success = false;
                try {
                    logger.info("deleting searchable snapshot [{}] from repository [{}]", snapshot, repository);
                    AcknowledgedResponse response = client.admin().cluster().prepareDeleteSnapshot(repository, snapshot.getName()).get();
                    if (response.isAcknowledged()) {
                        logger.info("searchable snapshots [{}] deleted from [{}]", snapshot, repository);
                        success = true;
                    }
                } catch (Exception e) {
                    logger.error("searchable snapshots [{}] NOT ACKED deleted from [{}]", e);
                } finally {
                    if (success == false) {
                        queue.add(snapshot);
                        Thread.sleep(3000);
                    }
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("searchable snapshots [{}] NOT ACKED deleted from [{}]", e);
        }
    }
}
