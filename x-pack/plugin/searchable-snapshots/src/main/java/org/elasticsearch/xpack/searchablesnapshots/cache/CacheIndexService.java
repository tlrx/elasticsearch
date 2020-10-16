/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.isSearchableSnapshotStore;

// TODO merge the internal task into CacheService
public class CacheIndexService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(CacheIndexService.class);

    private static final String CACHE_INDEX_DIRECTORY_NAME = "_cache";
    private static final String CACHE_ID_FIELD_NAME = "id";
    private static final String CACHE_STATE_FIELD_NAME = "cache_state";

    private static final XContent XCONTENT = XContentFactory.xContent(XContentType.JSON);

    public static final Setting<TimeValue> CACHE_INDEX_INTERVAL_SETTING = Setting.timeSetting(
        CacheService.SETTINGS_PREFIX + "index_interval",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(10),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final CacheFilesIndexerTask cacheIndexingTask;
    private final CacheService cacheService;
    private final Environment environment;
    private final Path[] nodeDataPaths;
    private final Path cacheIndexPath;
    private final Settings settings;

    private final SetOnce<Directory> cacheIndexDirectory = new SetOnce<>();
    private final SetOnce<IndexWriter> cacheIndexWriter = new SetOnce<>();

    public CacheIndexService(
        Environment environment,
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        CacheService cacheService
    ) {
        this.settings = settings;
        this.environment = environment;
        this.cacheService = cacheService;
        this.nodeDataPaths = nodeEnvironment.nodeDataPaths();
        this.cacheIndexPath = nodeDataPaths[0].resolve(CACHE_INDEX_DIRECTORY_NAME); // TODO handle multi data paths
        if (DiscoveryNode.isDataNode(settings)) {
            this.cacheIndexingTask = new CacheFilesIndexerTask(threadPool, CACHE_INDEX_INTERVAL_SETTING.get(settings));
            clusterService.getClusterSettings().addSettingsUpdateConsumer(CACHE_INDEX_INTERVAL_SETTING, this::setCacheIndexingInterval);
        } else {
            this.cacheIndexingTask = null;
        }
    }

    @Override
    protected void doStart() {
        final boolean isDataNode = DiscoveryNode.isDataNode(settings);
        final List<Document> documents = loadCacheIndex(isDataNode);
        if (isDataNode == false) {
            assert documents.isEmpty();
            try {
                IOUtils.rm(cacheIndexPath);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to delete cache index at [" + cacheIndexPath + ']', e);
            }
        } else {
            boolean success = false;
            final List<Closeable> closeables = new ArrayList<>();
            try {
                final Directory directory = new SimpleFSDirectory(cacheIndexPath);
                closeables.add(directory);

                final IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(new KeywordAnalyzer()));
                closeables.add(indexWriter);
                logger.debug("cleaning up previous cache index at [{}]", cacheIndexPath);
                indexWriter.deleteAll();
                if (documents.isEmpty() == false) {
                    indexWriter.addDocuments(documents);
                }
                commit(indexWriter);
                cacheIndexWriter.set(indexWriter);
                cacheIndexDirectory.set(directory);
                success = true;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to update cache index", e);
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(closeables);
                }
            }
        }
    }

    private void commit(final IndexWriter writer) throws IOException {
        final Map<String, String> commitData = new HashMap<>(1);
        commitData.put("version", Integer.toString(Version.CURRENT.id));
        writer.setLiveCommitData(commitData.entrySet());
        writer.prepareCommit();
        writer.commit();
    }

    @Override
    protected void doStop() {
        if (cacheIndexingTask != null) {
            cacheIndexingTask.setInterval(TimeValue.ZERO);
            cacheIndexingTask.close();
        }
        final IndexWriter indexWriter = cacheIndexWriter.get();
        if (indexWriter != null) {
            try {
                commit(indexWriter);
            } catch (IOException e) {
                logger.error("Failed to commit index writer on shutdown");
            }
        }
    }

    @Override
    protected void doClose() {
        IOUtils.closeWhileHandlingException(cacheIndexWriter.get(), cacheIndexDirectory.get());
    }

    private void setCacheIndexingInterval(TimeValue interval) {
        assert cacheIndexingTask != null;
        cacheIndexingTask.setInterval(interval);
    }

    private List<Document> loadCacheIndex(boolean isDataNode) {
        final List<Document> loadedCacheFiles = new ArrayList<>();
        if (FileSystemUtils.isAccessibleDirectory(cacheIndexPath, logger)) {
            try (Directory directory = new SimpleFSDirectory(cacheIndexPath); DirectoryReader reader = DirectoryReader.open(directory)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);

                final Query query = new MatchAllDocsQuery();
                final Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);

                for (LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                    final Scorer scorer = weight.scorer(leafReaderContext);
                    if (scorer != null) {
                        final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                        final IntPredicate isLiveDoc = liveDocs == null ? i -> true : liveDocs::get;
                        final DocIdSetIterator docIdSetIterator = scorer.iterator();
                        while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            if (isLiveDoc.test(docIdSetIterator.docID())) {
                                final Document document = leafReaderContext.reader().document(docIdSetIterator.docID());
                                final String[] documentId = document.getValues(CACHE_ID_FIELD_NAME);
                                if (documentId.length == 1) {
                                    final Path cacheFilePath = resolveCacheFile(documentId[0]);
                                    if (cacheFilePath == null) {
                                        logger.debug(
                                            "cache file index references file [{}] that no longer exists on disk, skipping",
                                            documentId[0]
                                        );
                                    } else if (isDataNode == false) {
                                        logger.debug("current node is not a data node, deleting cache file [{}] from disk", documentId[0]);
                                        Files.delete(cacheFilePath);
                                    } else {
                                        final BytesRef bytesRef = document.getBinaryValue(CACHE_STATE_FIELD_NAME);
                                        boolean success = false;
                                        try (
                                            XContentParser parser = XCONTENT.createParser(
                                                NamedXContentRegistry.EMPTY,
                                                LoggingDeprecationHandler.INSTANCE,
                                                bytesRef.bytes,
                                                bytesRef.offset,
                                                bytesRef.length
                                            )
                                        ) {
                                            logger.trace("loading state for cache file [{}]", cacheFilePath);
                                            final CacheState cacheState = CacheState.fromXContent(parser);
                                            logger.trace(
                                                "state loaded for cache file [{}]:\n{}\n",
                                                cacheFilePath.getFileName().toString(),
                                                cacheState
                                            );
                                            cacheService.put(
                                                new CacheKey(
                                                    cacheState.snapshotId,
                                                    cacheState.indexId,
                                                    cacheState.shardId,
                                                    cacheState.name
                                                ),
                                                cacheState.length,
                                                cacheFilePath,
                                                cacheState.ranges
                                            );
                                            success = true;
                                        } catch (Exception e) {
                                            logger.warn("failed to parse state for cache file [{}], skipping", documentId[0]);
                                        } finally {
                                            if (success) {
                                                loadedCacheFiles.add(document);
                                            }
                                        }
                                    }
                                }
                            }
                            // TODO check what happens if IOE here
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to load cache index at [" + cacheIndexPath + "]", e);
            }
        } else if (isDataNode) {
            try {
                Files.createDirectories(cacheIndexPath);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create cache index directory at [" + cacheIndexPath + "]", e);
            }
        }
        return loadedCacheFiles;
    }

    private Path resolveCacheFile(final String cacheFile) {
        for (Path nodeDataPath : nodeDataPaths) {
            final Path cacheFilePath = nodeDataPath.resolve(cacheFile.replaceFirst("../", ""));
            if (Files.exists(cacheFilePath)) {
                return cacheFilePath;
            }
        }
        return null;
    }

    /**
     * Reschedule the {@link CacheFilesIndexerTask} if the local data node is hosting searchable snapshot shards.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        assert DiscoveryNode.isDataNode(settings);
        if (event.routingTableChanged()) {
            final ClusterState state = event.state();
            final RoutingNode routingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
            assert routingNode != null;

            boolean hasSearchableSnapshotShards = false;
            for (ShardRouting shardRouting : routingNode) {
                if (shardRouting.active()) {
                    final IndexMetadata indexMetadata = state.metadata().getIndexSafe(shardRouting.index());
                    if (isSearchableSnapshotStore(indexMetadata.getSettings())) { // TODO should we limit this to open indices?
                        hasSearchableSnapshotShards = true;
                        break;
                    }
                }
            }
            if (hasSearchableSnapshotShards == false) {
                cacheIndexingTask.cancel();
            } else if (cacheIndexingTask.isScheduled() == false) {
                cacheIndexingTask.rescheduleIfNecessary();
            }
        }
    }

    public synchronized void syncCacheFiles() {
        try {
            final IndexWriter indexWriter = cacheIndexWriter.get();
            assert indexWriter != null;

            cacheService.forEachCacheFile(Objects::nonNull, (cacheKey, cacheFile) -> {
                final List<Tuple<Long, Long>> cacheFileState = cacheFile.getLastSyncedRanges(true);
                if (cacheFileState != null && cacheFileState.isEmpty() == false) {
                    try {
                        final String documentId = cacheIndexPath.relativize(cacheFile.getFile()).toString();
                        final Document document = createDocument(documentId, cacheKey, cacheFile, cacheFileState);
                        indexWriter.updateDocument(new Term(CACHE_ID_FIELD_NAME, documentId), document);
                    } catch (Exception e) {
                        logger.warn(
                            () -> new ParameterizedMessage("failed to update document for cache file [{}]", cacheFile.getFile()),
                            e
                        );
                    }
                }
            });

            if (indexWriter.hasUncommittedChanges()) {
                commit(indexWriter);
            }
        } catch (IOException e) {
            logger.warn("failed to collect cache files information", e);
        }
    }

    class CacheFilesIndexerTask extends AbstractAsyncTask {

        CacheFilesIndexerTask(ThreadPool threadPool, TimeValue interval) {
            super(logger, Objects.requireNonNull(threadPool), Objects.requireNonNull(interval), true);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        public void runInternal() {
            syncCacheFiles();
        }

        @Override
        public String toString() {
            return "cache_files_indexer";
        }
    }

    private static Document createDocument(
        String documentId,
        final CacheKey cacheKey,
        final CacheFile cacheFile,
        final List<Tuple<Long, Long>> cacheFileState
    ) throws IOException {
        final long[] ranges = new long[cacheFileState.size() * 2];
        for (int i = 0; i < cacheFileState.size(); i++) {
            Tuple<Long, Long> range = cacheFileState.get(i);
            ranges[i] = range.v1();
            ranges[i + 1] = range.v2();
        }

        final Document document = new Document();
        document.add(new StringField(CACHE_ID_FIELD_NAME, documentId, Field.Store.YES));
        try (BytesStreamOutput output = new BytesStreamOutput()) { // TODO use recycling instance?
            final CacheState cacheState = new CacheState(
                Version.CURRENT,
                cacheKey.getFileName(),
                cacheFile.getLength(),
                cacheKey.getSnapshotId(),
                cacheKey.getIndexId(),
                cacheKey.getShardId(),
                ranges
            );
            try (XContentBuilder builder = XContentFactory.contentBuilder(XCONTENT.type(), output)) {
                cacheState.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
            document.add(new StoredField(CACHE_STATE_FIELD_NAME, output.bytes().toBytesRef()));
        }

        return document;
    }

    private static class CacheState implements ToXContentObject {

        final Version version;
        final String name;
        final long length;
        final SnapshotId snapshotId;
        final IndexId indexId;
        final ShardId shardId;
        final long[] ranges;

        CacheState(Version version, String name, long length, SnapshotId snapshotId, IndexId indexId, ShardId shardId, long[] ranges) {
            this.version = version;
            this.name = name;
            this.length = length;
            this.snapshotId = snapshotId;
            this.indexId = indexId;
            this.shardId = shardId;
            this.ranges = ranges;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("version_id", version.id);
                builder.field("name", name);
                builder.field("length", length);
                builder.array("ranges", ranges);
                builder.field("snapshot_id", snapshotId);
                builder.field("index_id", indexId);
                builder.startObject("shard_id");
                {
                    builder.field("id", shardId.getId());
                    builder.field("index", shardId.getIndex());
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        static CacheState fromXContent(XContentParser parser) throws IOException {
            Version version = null;
            String name = null;
            Long length = null;
            long[] ranges = null;
            SnapshotId snapshotId = null;
            IndexId indexId = null;
            ShardId shardId = null;

            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                final String currentField = parser.currentName();
                switch (currentField) {
                    case "version_id":
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.nextToken(), parser);
                        version = Version.fromId(parser.intValue());
                        break;
                    case "name":
                        ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                        name = parser.text();
                        break;
                    case "length":
                        ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.nextToken(), parser);
                        length = parser.longValue();
                        break;
                    case "ranges":
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
                        List<Long> listOfRanges = new ArrayList<>();
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            listOfRanges.add(parser.longValue());
                        }
                        assert listOfRanges.isEmpty() == false;
                        assert listOfRanges.size() % 2 == 0;
                        ranges = listOfRanges.stream().mapToLong(l -> l).toArray();
                        break;
                    case "snapshot_id":
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        String snapshotName = null;
                        String snapshotUuid = null;
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                            switch (parser.currentName()) {
                                case "name":
                                    ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                                    snapshotName = parser.text();
                                    break;
                                case "uuid":
                                    ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                                    snapshotUuid = parser.text();
                                    break;
                                default:
                                    throwUnknownField(currentField, parser.getTokenLocation());
                            }
                        }
                        snapshotId = new SnapshotId(snapshotName, snapshotUuid);
                        break;
                    case "index_id":
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        String indexName = null;
                        String indexUuid = null;
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                            switch (parser.currentName()) {
                                case "name":
                                    ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                                    indexName = parser.text();
                                    break;
                                case "id":
                                    ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(), parser);
                                    indexUuid = parser.text();
                                    break;
                                default:
                                    throwUnknownField(currentField, parser.getTokenLocation());
                            }
                        }
                        indexId = new IndexId(indexName, indexUuid);
                        break;
                    case "shard_id":
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        Integer id = null;
                        Index index = null;
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                            switch (parser.currentName()) {
                                case "id":
                                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, parser.nextToken(), parser);
                                    id = parser.intValue();
                                    break;
                                case "index":
                                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                                    index = Index.fromXContent(parser);
                                    break;
                                default:
                                    throwUnknownField(currentField, parser.getTokenLocation());
                            }
                        }
                        shardId = new ShardId(index, id);
                        break;
                    default:
                        throwUnknownField(currentField, parser.getTokenLocation());
                }
            }
            assert parser.nextToken() == null;
            return new CacheState(version, name, length, snapshotId, indexId, shardId, ranges);
        }
    }
}
