/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportNodesListSearchableSnapshotsCaches;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportNodesListSearchableSnapshotsCaches.SearchableSnapshotsCacheNodeResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.TransportNodesListSearchableSnapshotsCaches.SearchableSnapshotsCachesNodesRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;

public class SearchableSnapshotAllocator implements ExistingShardsAllocator {

    private static final Logger logger = LogManager.getLogger(SearchableSnapshotAllocator.class);
    public static final String ALLOCATOR_NAME = "searchable_snapshot_allocator";

    private final ConcurrentMap<ShardId, AsyncShardFetch<SearchableSnapshotsCacheNodeResponse>> asyncFetchCache;
    private final NodeClient client;

    public SearchableSnapshotAllocator(NodeClient client) {
        this.client = client;
        this.asyncFetchCache = newConcurrentMap();
    }

    @Override
    public void beforeAllocation(RoutingAllocation allocation) {}

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        if (shardRouting.primary()
            && (shardRouting.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE
                || shardRouting.recoverySource().getType() == RecoverySource.Type.EMPTY_STORE)) {
            // we always force snapshot recovery source to use the snapshot-based recovery process on the node

            final Settings indexSettings = allocation.metadata().index(shardRouting.index()).getSettings();
            final IndexId indexId = new IndexId(
                SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings),
                SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
            );
            final SnapshotId snapshotId = new SnapshotId(
                SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
            );
            final String repository = SNAPSHOT_REPOSITORY_SETTING.get(indexSettings);
            final Snapshot snapshot = new Snapshot(repository, snapshotId);

            shardRouting = unassignedAllocationHandler.updateUnassigned(
                shardRouting.unassignedInfo(),
                new RecoverySource.SnapshotRecoverySource(
                    RecoverySource.SnapshotRecoverySource.NO_API_RESTORE_UUID,
                    snapshot,
                    Version.CURRENT,
                    indexId
                ),
                allocation.changes()
            );
        }

        final AllocateUnassignedDecision allocateUnassignedDecision = decideAllocation(allocation, shardRouting);
        if (allocateUnassignedDecision.isDecisionTaken() == false) {
            return;
        }

        if (allocateUnassignedDecision.getAllocationDecision() == AllocationDecision.YES) {
            unassignedAllocationHandler.initialize(
                allocateUnassignedDecision.getTargetNode().getId(),
                allocateUnassignedDecision.getAllocationId(),
                allocation.snapshotShardSizeInfo().getShardSize(shardRouting, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                allocation.changes()
            );
        } else {
            unassignedAllocationHandler.removeAndIgnore(allocateUnassignedDecision.getAllocationStatus(), allocation.changes());
        }
    }

    private AllocateUnassignedDecision decideAllocation(RoutingAllocation allocation, ShardRouting shardRouting) {
        assert shardRouting.unassigned();
        assert ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.get(
            allocation.metadata().getIndexSafe(shardRouting.index()).getSettings()
        ).equals(ALLOCATOR_NAME);

        if (shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
            if (allocation.snapshotShardSizeInfo().getShardSize(shardRouting) == null) {
                return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, null);
            }

            final AsyncShardFetch.FetchResult<SearchableSnapshotsCacheNodeResponse> cacheState = fetchCache(shardRouting, allocation);
            if (cacheState.hasData() == false) {
                allocation.setHasPendingAsyncFetch();
                return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, null);
            }
            final List<Map.Entry<DiscoveryNode, SearchableSnapshotsCacheNodeResponse>> cacheByNodes = cacheState.getData()
                .entrySet()
                .stream()
                .sorted((o1, o2) -> {
                    int compare = Long.compare(o2.getValue().getTotalLength(), o1.getValue().getTotalLength());
                    if (compare == 0) {
                        compare = o1.getKey().getId().compareTo(o2.getKey().getId());
                    }
                    return compare;
                })
                .collect(Collectors.toList());

            List<Tuple<DiscoveryNode, Decision>> noNodes = new ArrayList<>();
            List<Tuple<DiscoveryNode, Decision>> yesNodes = new ArrayList<>();
            List<Tuple<DiscoveryNode, Decision>> throttleNodes = new ArrayList<>();

            for (Map.Entry<DiscoveryNode, SearchableSnapshotsCacheNodeResponse> cacheNode : cacheByNodes) {
                final RoutingNode node = allocation.routingNodes().node(cacheNode.getKey().getId());
                if (node != null) {
                    final Decision decision = allocation.deciders().canAllocate(shardRouting, node, allocation);
                    if (decision.type() == Decision.Type.THROTTLE) {
                        throttleNodes.add(Tuple.tuple(cacheNode.getKey(), decision));
                    } else if (decision.type() == Decision.Type.NO) {
                        noNodes.add(Tuple.tuple(cacheNode.getKey(), decision));
                    } else {
                        yesNodes.add(Tuple.tuple(cacheNode.getKey(), decision));
                    }
                }
            }

            if (allocation.hasPendingAsyncFetch()) {
                return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA, null);
            } else if (yesNodes.isEmpty() == false) {
                final DiscoveryNode discoveryNode = yesNodes.get(0).v1();
                logger.debug("shard {} assigned to node [{}]", shardRouting.shardId(), discoveryNode);
                return AllocateUnassignedDecision.yes(discoveryNode, AllocationId.newInitializing().getId(), emptyList(), true);
            } else if (throttleNodes.isEmpty() == false) {
                // TODO compute decisions
                logger.debug("shard {} is throttled", shardRouting.shardId());
                return AllocateUnassignedDecision.throttle(emptyList());
            } else {
                // TODO compute decisions
                logger.debug("cannot assign shard {}", shardRouting.shardId());
                return AllocateUnassignedDecision.no(UnassignedInfo.AllocationStatus.DECIDERS_NO, emptyList());
            }
        }

        // let BalancedShardsAllocator take care of allocating this shard
        return AllocateUnassignedDecision.NOT_TAKEN;
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting shardRouting, RoutingAllocation routingAllocation) {
        assert shardRouting.unassigned();
        assert routingAllocation.debugDecision();
        return decideAllocation(routingAllocation, shardRouting);
    }

    @Override
    public void cleanCaches() {
        Releasables.close(asyncFetchCache.values());
        asyncFetchCache.clear();
    }

    @Override
    public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchCache.remove(startedShard.shardId()));
        }
    }

    @Override
    public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchCache.remove(failedShard.getRoutingEntry().shardId()));
        }
    }

    @Override
    public int getNumberOfInFlightFetches() {
        return asyncFetchCache.values().stream().mapToInt(AsyncShardFetch::getNumberOfInFlightFetches).sum();
    }

    private AsyncShardFetch.FetchResult<SearchableSnapshotsCacheNodeResponse> fetchCache(
        ShardRouting shardRouting,
        RoutingAllocation allocation
    ) {
        AsyncShardFetch.Lister<BaseNodesResponse<SearchableSnapshotsCacheNodeResponse>, SearchableSnapshotsCacheNodeResponse> lister = (
            shardId,
            customDataPath,
            nodes,
            listener) -> {
            final Settings indexSettings = allocation.metadata().index(shardRouting.index()).getSettings();
            final SnapshotId snapshotId = new SnapshotId(
                SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexSettings),
                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings)
            );
            final IndexId indexId = new IndexId(
                SNAPSHOT_INDEX_NAME_SETTING.get(indexSettings),
                SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
            );

            final SearchableSnapshotsCachesNodesRequest request = new SearchableSnapshotsCachesNodesRequest(
                snapshotId,
                indexId,
                shardId,
                nodes
            );
            client.executeLocally(
                TransportNodesListSearchableSnapshotsCaches.TYPE,
                request,
                ActionListener.wrap(listener::onResponse, listener::onFailure)
            );
        };

        final AsyncShardFetch<SearchableSnapshotsCacheNodeResponse> fetch = asyncFetchCache.computeIfAbsent(
            shardRouting.shardId(),
            shardId -> new AsyncShardFetch<>(logger, "cache_fetched", shardId, "", lister) {
                @Override
                protected void reroute(ShardId shardId, String reason) {
                    logger.trace("{} scheduling reroute for {}", shardId, reason);
                    client.execute(
                        ClusterRerouteAction.INSTANCE,
                        new ClusterRerouteRequest(),
                        ActionListener.wrap(
                            r -> logger.trace("{} scheduled reroute completed for {}", shardId, reason),
                            e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", shardId, reason), e)
                        )
                    );
                }
            }
        );

        final AsyncShardFetch.FetchResult<SearchableSnapshotsCacheNodeResponse> cacheState = fetch.fetchData(
            allocation.nodes(),
            allocation.getIgnoreNodes(shardRouting.shardId())
        );
        if (cacheState.hasData()) {
            cacheState.processAllocation(allocation);
        }
        return cacheState;
    }
}
