/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class TransportNodesListSearchableSnapshotsCaches extends TransportNodesAction<
    TransportNodesListSearchableSnapshotsCaches.SearchableSnapshotsCachesNodesRequest,
    TransportNodesListSearchableSnapshotsCaches.SearchableSnapshotsCachesNodesResponse,
    TransportNodesListSearchableSnapshotsCaches.SearchableSnapshotsCacheNodeRequest,
    TransportNodesListSearchableSnapshotsCaches.SearchableSnapshotsCacheNodeResponse> {

    public static final String ACTION_NAME = "indices:monitor/xpack/searchable_snapshots/cache";
    public static final ActionType<SearchableSnapshotsCachesNodesResponse> TYPE = new ActionType<>(
        ACTION_NAME,
        SearchableSnapshotsCachesNodesResponse::new
    );

    private final CacheService cacheService;

    @Inject
    public TransportNodesListSearchableSnapshotsCaches(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        CacheService cacheService
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            SearchableSnapshotsCachesNodesRequest::new,
            SearchableSnapshotsCacheNodeRequest::new,
            ThreadPool.Names.GENERIC,
            SearchableSnapshotsCacheNodeResponse.class
        );
        this.cacheService = Objects.requireNonNull(cacheService);
    }

    @Override
    protected SearchableSnapshotsCacheNodeResponse nodeOperation(SearchableSnapshotsCacheNodeRequest request, Task task) {
        final AtomicLong totalFiles = new AtomicLong(0L);
        final AtomicLong totalLength = new AtomicLong(0L);
        cacheService.forEachCacheFile(
            cacheKey -> request.getSnapshotId().equals(cacheKey.getSnapshotId())
                && request.getIndexId().equals(cacheKey.getIndexId())
                && request.getShardId().equals(cacheKey.getShardId()),
            (cacheKey, cacheFile) -> {
                totalFiles.incrementAndGet();
                final List<Tuple<Long, Long>> lastCompletedRanges = cacheFile.getLastSyncedRanges(false);
                if (lastCompletedRanges != null) {
                    lastCompletedRanges.forEach(tuple -> totalLength.addAndGet(tuple.v2() - tuple.v1()));
                }
            }
        );
        return new SearchableSnapshotsCacheNodeResponse(clusterService.localNode(), totalFiles.get(), totalLength.get());
    }

    @Override
    protected SearchableSnapshotsCachesNodesResponse newResponse(
        SearchableSnapshotsCachesNodesRequest request,
        List<SearchableSnapshotsCacheNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new SearchableSnapshotsCachesNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected SearchableSnapshotsCacheNodeRequest newNodeRequest(SearchableSnapshotsCachesNodesRequest request) {
        return new SearchableSnapshotsCacheNodeRequest(request);
    }

    @Override
    protected SearchableSnapshotsCacheNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new SearchableSnapshotsCacheNodeResponse(in);
    }

    public static class SearchableSnapshotsCachesNodesRequest extends BaseNodesRequest<SearchableSnapshotsCachesNodesRequest> {

        private final SnapshotId snapshotId;
        private final IndexId indexId;
        private final ShardId shardId;

        public SearchableSnapshotsCachesNodesRequest(StreamInput in) throws IOException {
            super(in);
            snapshotId = new SnapshotId(in);
            indexId = new IndexId(in);
            shardId = new ShardId(in);
        }

        public SearchableSnapshotsCachesNodesRequest(SnapshotId snapshotId, IndexId indexId, ShardId shardId, DiscoveryNode[] nodes) {
            super(nodes);
            this.snapshotId = Objects.requireNonNull(snapshotId);
            this.indexId = Objects.requireNonNull(indexId);
            this.shardId = Objects.requireNonNull(shardId);
        }

        public SnapshotId getSnapshotId() {
            return snapshotId;
        }

        public IndexId getIndexId() {
            return indexId;
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshotId.writeTo(out);
            indexId.writeTo(out);
            shardId.writeTo(out);
        }
    }

    public static class SearchableSnapshotsCachesNodesResponse extends BaseNodesResponse<SearchableSnapshotsCacheNodeResponse> {

        public SearchableSnapshotsCachesNodesResponse(StreamInput in) throws IOException {
            super(in);
        }

        public SearchableSnapshotsCachesNodesResponse(
            ClusterName clusterName,
            List<SearchableSnapshotsCacheNodeResponse> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<SearchableSnapshotsCacheNodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(SearchableSnapshotsCacheNodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<SearchableSnapshotsCacheNodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    public static class SearchableSnapshotsCacheNodeRequest extends TransportRequest {

        private final SnapshotId snapshotId;
        private final IndexId indexId;
        private final ShardId shardId;

        public SearchableSnapshotsCacheNodeRequest(StreamInput in) throws IOException {
            super(in);
            snapshotId = new SnapshotId(in);
            indexId = new IndexId(in);
            shardId = new ShardId(in);
        }

        public SearchableSnapshotsCacheNodeRequest(final SearchableSnapshotsCachesNodesRequest request) {
            this.snapshotId = request.getSnapshotId();
            this.indexId = request.getIndexId();
            this.shardId = request.getShardId();
        }

        public SnapshotId getSnapshotId() {
            return snapshotId;
        }

        public IndexId getIndexId() {
            return indexId;
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            snapshotId.writeTo(out);
            indexId.writeTo(out);
            shardId.writeTo(out);
        }
    }

    public static class SearchableSnapshotsCacheNodeResponse extends BaseNodeResponse {

        private final long totalFiles;
        private final long totalLength;

        public SearchableSnapshotsCacheNodeResponse(StreamInput in) throws IOException {
            super(in);
            totalFiles = in.readVLong();
            totalLength = in.readVLong();
        }

        public SearchableSnapshotsCacheNodeResponse(DiscoveryNode node, long totalFiles, long totalLength) {
            super(node);
            this.totalFiles = totalFiles;
            this.totalLength = totalLength;
        }

        public long getTotalFiles() {
            return totalFiles;
        }

        public long getTotalLength() {
            return totalLength;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(totalFiles);
            out.writeVLong(totalLength);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SearchableSnapshotsCacheNodeResponse that = (SearchableSnapshotsCacheNodeResponse) o;
            return totalFiles == that.totalFiles && totalLength == that.totalLength && Objects.equals(getNode(), that.getNode());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNode(), totalFiles, totalLength);
        }
    }
}
