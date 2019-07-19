/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.recovery;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class AutoRestoreWithClusterStateListenerIT extends AutoRestoreIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.nodePlugins());
        classes.add(TestPlugin.class);
        return classes;
    }

    public static final class TestPlugin extends AutoRestoreTestPlugin implements ClusterPlugin {

        @Override
        public Collection<Object> createComponents(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry,
            final Environment environment,
            final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry) {

            return List.of(new TestClusterStateListener(clusterService, threadPool, client));
        }

        static class TestClusterStateListener extends AbstractLifecycleComponent implements ClusterStateListener {

            private static final Logger logger = LogManager.getLogger(TestClusterStateListener.class);

            private final ClusterService clusterService;
            private final ThreadPool threadPool;
            private final Client client;

            public TestClusterStateListener(final ClusterService clusterService, final ThreadPool threadPool, final Client client) {
                this.clusterService = clusterService;
                this.threadPool = threadPool;
                this.client = client;
            }

            @Override
            protected void doStart() {
                clusterService.addListener(this);
            }

            @Override
            protected void doStop() {
                clusterService.removeListener(this);
            }

            @Override
            protected void doClose() throws IOException {
            }

            @Override
            public void clusterChanged(final ClusterChangedEvent event) {
                if (event.state().getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                    return;
                }

                if (event.localNodeMaster() && event.routingTableChanged()) {
                    final Set<Index> indices = listIndices(event.state());
                    if (indices.isEmpty() == false) {
                        clusterService.submitStateUpdateTask("found [" + indices + "] indices with unassigned shards",
                            new ClusterStateUpdateTask() {

                                boolean changed = false;

                                @Override
                                public ClusterState execute(final ClusterState currentState) throws Exception {
                                    final Set<Index> indices = listIndices(currentState);
                                    if (indices.isEmpty()) {
                                        return currentState;
                                    }

                                    final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                                    for (Index index : indices) {
                                        final IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);

                                        RecoverySource source = null;
                                        for (IndexShardRoutingTable shardRoutingTable : currentState.routingTable().index(index)) {
                                            ShardRouting primary = shardRoutingTable.primaryShard();
                                            UnassignedInfo info = primary.unassignedInfo();
                                            if (primary.unassigned()
                                                && info.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY) {

                                                if (source == null) {
                                                    Settings indexSettings = currentState.metaData().index(index).getSettings();
                                                    String repository = RESTORE_FROM_SNAPSHOT_REPOSITORY_NAME.get(indexSettings);
                                                    String name = RESTORE_FROM_SNAPSHOT_SNAPSHOT_NAME.get(indexSettings);
                                                    String id = RESTORE_FROM_SNAPSHOT_SNAPSHOT_ID.get(indexSettings);
                                                    Version version = RESTORE_FROM_SNAPSHOT_SNAPSHOT_VERSION.get(indexSettings);
                                                    String indexId = RESTORE_FROM_SNAPSHOT_SNAPSHOT_INDEX.get(indexSettings);

                                                    Snapshot snapshot = new Snapshot(repository, new SnapshotId(name, id));
                                                    source = new SnapshotRecoverySource(repository, snapshot, version, indexId);
                                                }

                                                if (changed == false) {
                                                    changed = source.equals(primary.recoverySource()) == false;
                                                }
                                                indexRoutingTableBuilder.addShard(primary.updateUnassigned(info, source));
                                                shardRoutingTable.replicaShards().forEach(indexRoutingTableBuilder::addShard);

                                            } else {
                                                indexRoutingTableBuilder.addIndexShard(shardRoutingTable);
                                            }
                                        }
                                        routingTableBuilder.remove(index.getName()).add(indexRoutingTableBuilder.build());
                                    }
                                    if (changed == false) {
                                        return currentState;
                                    }
                                    return ClusterState.builder(currentState).routingTable(routingTableBuilder.build()).build();
                                }

                                @Override
                                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                    if (changed) {
                                        // NORELEASE Fix this
                                        // AllocationService is not exposed to plugins, neither it is accessible through ClusterService
                                        // so we need to trigger a follow up reroute...
                                        threadPool.generic().submit(() -> client.admin().cluster().prepareReroute().get());
                                    }
                                }

                                @Override
                                public void onFailure(String source, Exception e) {
                                    logger.warn("failed to update recovery sources", e);
                                }
                            }
                        );
                    }
                }
            }

            /**
             * Returns all indices that have RESTORE_FROM_SNAPSHOT set to true and at least one unassigned primary shard with no valid copy
             */
            private static Set<Index> listIndices(final ClusterState clusterState) {
                final Set<Index> indices = new HashSet<>();
                for (ObjectObjectCursor<String, IndexMetaData> cursor : clusterState.metaData().indices()) {
                    IndexMetaData indexMetaData = cursor.value;
                    if (RESTORE_FROM_SNAPSHOT.get(indexMetaData.getSettings())) {
                        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexMetaData.getIndex());
                        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                            if (shardRoutingTable.primaryShard().unassigned()) {
                                UnassignedInfo unassignedInfo = shardRoutingTable.primaryShard().unassignedInfo();
                                if (unassignedInfo.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY) {
                                    indices.add(indexMetaData.getIndex());
                                    break;
                                }
                            }
                        }
                    }
                }
                return indices;
            }
        }
    }
}
