/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static org.elasticsearch.blobstore.cache.CachedBlobContainer.DEFAULT_BYTE_ARRAY_SIZE;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOT_FORMAT;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class SearchableSnapshotsBlobStoreCacheIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(WaitForSnapshotBlobCacheShardsActivePlugin.class);
        plugins.add(TrackingRepositoryPlugin.class);
        plugins.addAll(super.nodePlugins());
        return List.copyOf(plugins);
    }

    @Override
    protected boolean useRandomCacheSettings() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // aligning cache range size with the size of cached data in the snapshot blob store make this test easier to understand.
            // TODO still, it means that we don't test all code paths in CachedBlobContainer
            .put(CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(), new ByteSizeValue(DEFAULT_BYTE_ARRAY_SIZE, ByteSizeUnit.BYTES))
            .build();
    }

    public void testBlobStoreCache() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), "false")
            )
        );
        ensureGreen(indexName);
        final int numberOfPrimaries = getNumShards(indexName).numPrimaries;

        final int nbDocsPerShards = scaledRandomIntBetween(1, 100);
        indexRandomDocsPerShard(indexName, nbDocsPerShards);
        flush(indexName);
        indexRandomDocsPerShard(indexName, nbDocsPerShards);
        flushAndRefresh(indexName);

        final ForceMergeResponse forceMergeResponse = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMergeResponse.getSuccessfulShards(), equalTo(numberOfPrimaries));
        assertThat(forceMergeResponse.getFailedShards(), equalTo(0));

        final long nbDocs = (2 * nbDocsPerShards) * numberOfPrimaries;

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Path repositoryLocation = randomRepoPath();
        createFsRepository(repositoryName, repositoryLocation);

        final SnapshotId snapshot = createSnapshot(repositoryName, List.of(indexName));
        final String snapshotMetadata = "snap-" + snapshot.getUUID() + ".dat";

        // extract the list of blobs of the snapshot from the repository on disk
        final Map<Integer, BlobStoreIndexShardSnapshot> blobsInSnapshot = blobsInSnapshot(repositoryLocation, snapshotMetadata);
        assertThat(blobsInSnapshot.size(), equalTo(numberOfPrimaries));
        assertFalse(
            "Compound file segment makes this test harder to reason about and debug so we prefer to fail",
            blobsInSnapshot.values()
                .stream()
                .flatMap(blobs -> blobs.indexFiles().stream())
                .map(blob -> IndexFileNames.getExtension(blob.physicalName()))
                .anyMatch(ext -> "cfs".equals(ext) || "cfe".equals(ext))
        );
        final long expectedNumberOfCachedBlobs = numberOfCachedBlobs(blobsInSnapshot);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        // register a new repository that can track blob read operations
        assertAcked(client().admin().cluster().prepareDeleteRepository(repositoryName));
        createRepository(
            repositoryName,
            TrackingRepositoryPlugin.TRACKING,
            Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), repositoryLocation).build(),
            false
        );
        assertBusy(this::ensureClusterStateConsistency);

        expectThrows(
            IndexNotFoundException.class,
            ".snapshot-blob-cache system index should not be created yet",
            () -> systemClient().admin().indices().prepareGetIndex().addIndices(SNAPSHOT_BLOB_CACHE_INDEX).get()
        );

        logger.info("--> mount snapshot [{}] as an index for the first time", snapshot);
        final String restoredIndex = mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false) // TODO also test with prewarming
                .put(SearchableSnapshots.SNAPSHOT_BLOB_STORE_CACHE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(restoredIndex);

        assertExecutorAreIdle();
        ensureBlobStoreRepositoriesWithActiveShards(
            restoredIndex,
            (nodeId, blobStore) -> assertThat(
                "Blob read operations should have been executed on node [" + nodeId + ']',
                blobStore.numberOfReads(),
                greaterThan(0L)
            )
        );

        logger.info("--> verifying cached documents in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, snapshotMetadata, blobsInSnapshot, expectedNumberOfCachedBlobs);

        logger.info("--> verifying documents in index [{}]", restoredIndex);
        assertHitCount(client().prepareSearch(restoredIndex).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        ensureAllBlobStoreRepositories((node, blobStore) -> blobStore.reset());
        assertAcked(client().admin().indices().prepareDelete(restoredIndex));

        logger.info("--> mount snapshot [{}] as an index for the second time", snapshot);
        final String restoredAgainIndex = mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false) // TODO also test with prewarming
                .put(SearchableSnapshots.SNAPSHOT_BLOB_STORE_CACHE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(restoredAgainIndex);

        assertExecutorAreIdle();
        ensureBlobStoreRepositoriesWithActiveShards(restoredAgainIndex, (nodeId, blobStore) -> {
            for (Map.Entry<String, AtomicLong> blob : blobStore.blobs.entrySet()) {
                // Those files are accessed when recovering from the snapshot during restore and do not benefit from
                // the snapshot blob cache as the files are accessed outside of a searchable snapshot shard
                assertThat(blob.getKey(), anyOf(startsWith("index-"), containsString("meta-"), endsWith(snapshotMetadata)));
            }
        });

        logger.info("--> verifying cached documents (again) in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, snapshotMetadata, blobsInSnapshot, expectedNumberOfCachedBlobs);

        logger.info("--> verifying documents in index [{}]", restoredAgainIndex);
        assertHitCount(client().prepareSearch(restoredAgainIndex).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        ensureAllBlobStoreRepositories((node, blobStore) -> blobStore.reset());

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen(restoredAgainIndex);

        logger.info("--> verifying cached documents (after restart) in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, snapshotMetadata, blobsInSnapshot, expectedNumberOfCachedBlobs);

        // Without the WaitForSnapshotBlobCacheShardsActivePlugin this would fail
        ensureBlobStoreRepositoriesWithActiveShards(restoredAgainIndex, (nodeId, blobStore) -> {
            for (Map.Entry<String, AtomicLong> blob : blobStore.blobs.entrySet()) {
                // Those files are accessed during restore while recovering from the snapshot
                assertThat(blob.getKey(), anyOf(startsWith("index-"), containsString("meta-"), endsWith(snapshotMetadata)));
            }
        });
    }

    /**
     * @return a {@link Client} that can be used to query the blob store cache system index
     */
    private Client systemClient() {
        return new OriginSettingClient(client(), ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN);
    }

    private void refreshSystemIndex() {
        final RefreshResponse refreshResponse = systemClient().admin().indices().prepareRefresh(SNAPSHOT_BLOB_CACHE_INDEX).get();
        assertThat(refreshResponse.getSuccessfulShards(), greaterThan(0));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
    }

    private void assertCachedBlobsInSystemIndex(
        String repositoryName,
        String snapshotMetadata,
        Map<Integer, BlobStoreIndexShardSnapshot> blobsInSnapshot,
        long numberOfCachedBlobs
    ) throws Exception {

        assertBusy(() -> {
            refreshSystemIndex();

            final SearchResponse searchResponse = systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get();
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(numberOfCachedBlobs));

            assertThat(
                "A snapshot metadata file ["
                    + snapshotMetadata
                    + "] should have been indexed for each shard in the blob cache system index",
                systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX)
                    .setQuery(
                        QueryBuilders.boolQuery()
                            .must(QueryBuilders.termQuery("repository", repositoryName))
                            .must(QueryBuilders.termQuery("blob.name", snapshotMetadata))
                    )
                    .setSize(0)
                    .get()
                    .getHits()
                    .getTotalHits().value,
                equalTo((long) blobsInSnapshot.size())
            );

            for (BlobStoreIndexShardSnapshot blobs : blobsInSnapshot.values()) {
                for (BlobStoreIndexShardSnapshot.FileInfo blob : blobs.indexFiles()) {
                    final String blobName = blob.partName(0L);
                    if (blobName.startsWith(UPLOADED_DATA_BLOB_PREFIX) == false) {
                        continue;
                    }

                    final SearchResponse blobSearchResponse = systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX)
                        .setQuery(
                            QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("repository", repositoryName))
                                .must(QueryBuilders.termQuery("blob.name", blobName))
                        )
                        .get();

                    assertThat(
                        "Blob [" + blobName + "] should have been indexed in the blob cache system index",
                        blobSearchResponse.getHits().getTotalHits().value,
                        blob.length() <= DEFAULT_BYTE_ARRAY_SIZE ? equalTo(1L) : equalTo(2L)
                    );

                    final CachedBlob cachedBlob = CachedBlob.fromSource(blobSearchResponse.getHits().getAt(0).getSourceAsMap());
                    assertThat("Unable to parse cached blob [" + blobName + ']', notNullValue());
                    assertThat(cachedBlob.length(), equalTo(Math.min(blob.length(), DEFAULT_BYTE_ARRAY_SIZE)));
                    assertThat(cachedBlob.offset(), anyOf(equalTo(0L), equalTo(blob.length() - DEFAULT_BYTE_ARRAY_SIZE)));
                    // TODO verify version, type etc
                }
            }

        });
    }

    /**
     * Returns the {@link TrackingRepositoryPlugin} instance on a given node.
     */
    private TrackingRepositoryPlugin getTrackingRepositoryInstance(String node) {
        DiscoveryNode discoveryNode = clusterService().state().nodes().resolveNode(node);
        assertThat("Cannot find node " + node, discoveryNode, notNullValue());

        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, discoveryNode.getName());
        assertThat("Cannot find PluginsService on node " + node, pluginsService, notNullValue());

        List<TrackingRepositoryPlugin> trackingRepositoryPlugins = pluginsService.filterPlugins(TrackingRepositoryPlugin.class);
        assertThat("List of TrackingRepositoryPlugin is null on node " + node, trackingRepositoryPlugins, notNullValue());
        assertThat("List of TrackingRepositoryPlugin is empty on node " + node, trackingRepositoryPlugins, hasSize(1));

        TrackingRepositoryPlugin trackingRepositoryPlugin = trackingRepositoryPlugins.get(0);
        assertThat("TrackingRepositoryPlugin is null on node " + node, trackingRepositoryPlugin, notNullValue());
        return trackingRepositoryPlugin;
    }

    private void ensureAllBlobStoreRepositories(BiConsumer<String, TrackingRepositoryPlugin> consumer) {
        for (String nodeName : internalCluster().getNodeNames()) {
            consumer.accept(nodeName, getTrackingRepositoryInstance(nodeName));
        }
    }

    private void ensureBlobStoreRepositoriesWithActiveShards(String indexName, BiConsumer<String, TrackingRepositoryPlugin> consumer) {
        final ClusterState clusterState = clusterService().state();
        assertTrue(clusterState.metadata().hasIndex(indexName));
        assertTrue(SearchableSnapshotsConstants.isSearchableSnapshotStore(clusterState.metadata().index(indexName).getSettings()));
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        assertThat(indexRoutingTable, notNullValue());

        final ShardsIterator shardsIterator = indexRoutingTable.randomAllActiveShardsIt();
        assertThat(shardsIterator.size(), greaterThanOrEqualTo(getNumShards(indexName).numPrimaries));

        for (ShardRouting shardRouting : shardsIterator) {
            consumer.accept(shardRouting.currentNodeId(), getTrackingRepositoryInstance(shardRouting.currentNodeId()));
        }
    }

    private Map<Integer, BlobStoreIndexShardSnapshot> blobsInSnapshot(Path repositoryLocation, String snapshotMetadata) throws IOException {
        final Map<Integer, BlobStoreIndexShardSnapshot> blobsPerShard = new HashMap<>();
        Files.walkFileTree(repositoryLocation.resolve("indices"), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                final String fileName = file.getFileName().toString();
                if (fileName.equals(snapshotMetadata)) {
                    blobsPerShard.put(
                        Integer.parseInt(file.getParent().getFileName().toString()),
                        INDEX_SHARD_SNAPSHOT_FORMAT.deserialize(fileName, xContentRegistry(), Streams.readFully(Files.newInputStream(file)))
                    );
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return Map.copyOf(blobsPerShard);
    }

    private long numberOfCachedBlobs(Map<Integer, BlobStoreIndexShardSnapshot> blobsInSnapshot) {
        long expectedCachedBlobs = blobsInSnapshot.size(); // 1 snapshot metadata file per shard
        for (BlobStoreIndexShardSnapshot blobs : blobsInSnapshot.values()) {
            for (BlobStoreIndexShardSnapshot.FileInfo blob : blobs.indexFiles()) {
                if (blob.name().startsWith(UPLOADED_DATA_BLOB_PREFIX)) {
                    if (blob.length() <= DEFAULT_BYTE_ARRAY_SIZE) {
                        expectedCachedBlobs += 1L;
                    } else {
                        expectedCachedBlobs += 2L;
                    }
                }
            }
        }
        return expectedCachedBlobs;
    }

    private void assertExecutorAreIdle() throws Exception {
        assertBusy(
            () -> List.of(
                SearchableSnapshotsConstants.CACHE_FETCH_ASYNC_THREAD_POOL_NAME,
                SearchableSnapshotsConstants.CACHE_PREWARMING_THREAD_POOL_NAME
            ).forEach(threadPoolName -> clusterService().state().nodes().forEach(discoveryNode -> {
                final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, discoveryNode.getName());
                final ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.executor(threadPoolName);
                assertThat(executor.getQueue().size(), equalTo(0));
                assertThat(executor.getActiveCount(), equalTo(0));
            }))
        );
    }

    protected void indexRandomDocsPerShard(String indexName, int nbDocsPerShard) throws Exception {
        final int numShards = getNumShards(indexName).numPrimaries;
        final CountDownLatch latch = new CountDownLatch(numShards);
        for (int i = 0; i < numShards; i++) {
            final int shard = i;
            final BulkRequest bulkRequest = IntStream.range(0, nbDocsPerShard)
                .mapToObj(
                    n -> new IndexRequest().source(
                        "random_text",
                        randomAlphaOfLength(20),
                        "random_long",
                        randomLong(),
                        "random_bool",
                        randomBoolean()
                    ).routing(String.valueOf(shard))
                )
                .collect(() -> new BulkRequest(indexName), BulkRequest::add, (b1, b2) -> b1.add(b2.requests()));

            client().bulk(bulkRequest, ActionListener.wrap(latch::countDown));
        }
        latch.await(30L, TimeUnit.SECONDS);
    }

    /**
     * A plugin that allows to track the total number of bytes read on blobs
     */
    public static class TrackingRepositoryPlugin extends Plugin implements RepositoryPlugin {

        static final String TRACKING = "tracking";

        private final Map<String, AtomicLong> blobs = new ConcurrentHashMap<>();

        long numberOfReads() {
            return blobs.values().stream().mapToLong(AtomicLong::get).sum();
        }

        void reset() {
            blobs.clear();
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                TRACKING,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {

                    @Override
                    protected BlobStore createBlobStore() throws Exception {
                        final BlobStore delegate = super.createBlobStore();
                        return new BlobStore() {
                            @Override
                            public BlobContainer blobContainer(BlobPath path) {
                                return new TrackingFilesBlobContainer(delegate.blobContainer(path));
                            }

                            @Override
                            public void close() throws IOException {
                                delegate.close();
                            }
                        };
                    }
                }
            );
        }

        class TrackingFilesBlobContainer extends FilterBlobContainer {

            TrackingFilesBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            public InputStream readBlob(String blobName) throws IOException {
                return new CountingInputStream(path().buildAsString() + blobName, super.readBlob(blobName));
            }

            @Override
            public InputStream readBlob(String blobName, long position, long length) throws IOException {
                return new CountingInputStream(path().buildAsString() + blobName, super.readBlob(blobName, position, length));
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new TrackingFilesBlobContainer(child);
            }
        }

        class CountingInputStream extends FilterInputStream {

            private final String blob;
            long bytesRead = 0L;

            protected CountingInputStream(String blobName, InputStream in) {
                super(in);
                this.blob = blobName;
            }

            @Override
            public int read() throws IOException {
                final int result = in.read();
                if (result == -1) {
                    return result;
                }
                bytesRead += 1L;
                return result;
            }

            @Override
            public int read(byte[] b, int offset, int len) throws IOException {
                final int result = in.read(b, offset, len);
                if (result == -1) {
                    return result;
                }
                bytesRead += len;
                return result;
            }

            @Override
            public void close() throws IOException {
                blobs.computeIfAbsent(blob, s -> new AtomicLong(bytesRead)).addAndGet(bytesRead);
                super.close();
            }
        }
    }

    /**
     * This plugin declares an {@link AllocationDecider} that forces searchable snapshot shards to be allocated after
     * the primary shards of the snapshot blob cache index are started. This way we can ensure that searchable snapshot
     * shards can use the snapshot blob cache index after the cluster restarted.
     */
    public static class WaitForSnapshotBlobCacheShardsActivePlugin extends Plugin implements ClusterPlugin {

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            final String name = "wait_for_snapshot_blob_cache_shards_active";
            return List.of(new AllocationDecider() {

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return canAllocate(shardRouting, allocation);
                }

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                    final IndexMetadata indexMetadata = allocation.metadata().index(shardRouting.index());
                    if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexMetadata.getSettings()) == false) {
                        return allocation.decision(Decision.YES, name, "index is not a searchable snapshot shard - can allocate");
                    }
                    if (allocation.metadata().hasIndex(SNAPSHOT_BLOB_CACHE_INDEX) == false) {
                        return allocation.decision(Decision.YES, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not created yet");
                    }
                    if (allocation.routingTable().hasIndex(SNAPSHOT_BLOB_CACHE_INDEX) == false) {
                        return allocation.decision(Decision.THROTTLE, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not active yet");
                    }
                    final IndexRoutingTable indexRoutingTable = allocation.routingTable().index(SNAPSHOT_BLOB_CACHE_INDEX);
                    if (indexRoutingTable.allPrimaryShardsActive() == false) {
                        return allocation.decision(Decision.THROTTLE, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not active yet");
                    }
                    return allocation.decision(Decision.YES, name, "primary shard for this replica is already active");
                }
            });
        }
    }
}
