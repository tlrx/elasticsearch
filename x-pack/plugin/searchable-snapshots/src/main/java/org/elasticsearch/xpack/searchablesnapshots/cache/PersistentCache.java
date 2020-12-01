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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSortedSet;
import static org.elasticsearch.xpack.searchablesnapshots.cache.CacheService.getShardCachePath;

public class PersistentCache implements Closeable {

    private static final Logger logger = LogManager.getLogger(PersistentCache.class);

    private static final String NODE_VERSION_COMMIT_KEY = "node_version";

    private final NodeEnvironment nodeEnvironment;
    private final List<CacheIndexWriter> writers;
    private final AtomicBoolean closed;

    public PersistentCache(NodeEnvironment nodeEnvironment) {
        this.writers = createWriters(nodeEnvironment);
        this.nodeEnvironment = nodeEnvironment;
        this.closed = new AtomicBoolean();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("Persistent cache is already closed");
        }
    }

    /**
     * @return the {@link CacheIndexWriter} to use for the given {@link CacheFile}
     */
    private CacheIndexWriter getWriter(CacheFile cacheFile) {
        ensureOpen();
        if (writers.size() == 1) {
            return writers.get(0);
        } else {
            final Path path = cacheFile.getFile().toAbsolutePath();
            return writers.stream()
                .filter(writer -> path.startsWith(writer.nodePath().path))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Failed to find a Lucene index for cache file path [" + path + ']'));
        }
    }

    public void addCacheFile(CacheFile cacheFile, SortedSet<Tuple<Long, Long>> ranges) throws IOException {
        getWriter(cacheFile).updateCacheFile(cacheFile, ranges);
    }

    public void removeCacheFile(CacheFile cacheFile) throws IOException {
        getWriter(cacheFile).deleteCacheFile(cacheFile);
    }

    /**
     * This method repopulates the {@link CacheService} by looking at the files on the disk and for each file found, retrieves the latest
     * synchronized information from Lucene indices.
     *
     * This method iterates over all node data paths and all shard directories in order to found the "snapshot_cache" directories that
     * contain the cache files. When such a directory is found, the method iterates over the cache files and searches their name/UUID in
     * the existing Lucene index (the list of {@link CacheIndexSearcher}; usually there's only one). If no information is found (ie no
     * matching docs in the Lucene index) then the file is deleted from disk. If a matching doc is found the stored fields are extracted
     * from the Lucene document and are used to rebuild the necessary {@link CacheKey}, {@link SnapshotId}, {@link IndexId}, {@link ShardId}
     * and cache file ranges objects. The matching Lucene document is then indexed in the new persistent cache index (the current
     * {@link CacheIndexWriter}) and the cache file is added back to the searchable snapshots cache service again. Note that adding cache
     * file to the cache service might trigger evictions so previously indexed Lucene documents might be delete again (see
     * {@link CacheService#onCacheFileRemoval(CacheFile)} which calls {@link #removeCacheFile(CacheFile)}.
     *
     * @param cacheService the {@link CacheService} to use when repopulating {@link CacheFile}.
     */
    void loadCacheFiles(CacheService cacheService) {
        ensureOpen();
        try {
            final List<CacheIndexSearcher> deletes = new ArrayList<>();
            boolean success = false;
            try {
                for (CacheIndexWriter writer : writers) {
                    final NodeEnvironment.NodePath nodePath = writer.nodePath();
                    logger.debug("loading persistent cache on data path [{}]", nodePath);

                    final List<CacheIndexSearcher> searchers = createSearchers(nodePath, writer.directoryPath());
                    logger.trace("found [{}] existing persistent cache indices on data path [{}]", searchers.size(), nodePath);
                    deletes.addAll(searchers);

                    for (String indexUUID : nodeEnvironment.availableIndexFoldersForPath(writer.nodePath())) {
                        for (ShardId shardId : nodeEnvironment.findAllShardIds(new Index("_unknown_", indexUUID))) {
                            final Path shardDataPath = writer.nodePath().resolve(shardId);
                            final Path shardCachePath = getShardCachePath(new ShardPath(false, shardDataPath, shardDataPath, shardId));

                            if (Files.isDirectory(shardCachePath)) {
                                logger.trace("found snapshot cache dir at [{}], loading cache files from disk and index", shardCachePath);
                                Files.walkFileTree(shardCachePath, new CacheFileVisitor(cacheService, writer, searchers));
                            }
                        }
                    }
                }
                for (CacheIndexWriter writer : writers) {
                    writer.prepareCommit();
                }
                for (CacheIndexWriter writer : writers) {
                    writer.commit();
                }
                logger.info("persistent cache index loaded");
                success = true;
            } finally {
                if (success) {
                    for (CacheIndexSearcher searcher : deletes) {
                        try {
                            IOUtils.close(searcher);
                        } catch (Exception e) {
                            logger.warn(() -> new ParameterizedMessage("failed to close persistent cache index [{}]", searcher), e);
                        } finally {
                            try {
                                logger.debug("deleting persistent cache index at [{}]", searcher.directoryPath());
                                IOUtils.rm(searcher.directoryPath());
                            } catch (Exception e) {
                                logger.warn(() -> new ParameterizedMessage("failed to delete persistent cache index [{}]", searcher), e);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            try {
                close();
            } catch (Exception e2) {
                logger.warn("failed to close persistent cache index", e2);
                e.addSuppressed(e2);
            }
            throw new UncheckedIOException("Failed to load persistent cache", e);
        } finally {
            closeIfAnyIndexWriterHasTragedyOrIsClosed();
        }
    }

    void commit() throws IOException {
        ensureOpen();
        try {
            for (CacheIndexWriter writer : writers) {
                writer.prepareCommit();
            }
            for (CacheIndexWriter writer : writers) {
                writer.commit();
            }
        } catch (IOException e) {
            try {
                close();
            } catch (Exception e2) {
                logger.warn("failed to close persistent cache index writer", e2);
                e.addSuppressed(e2);
            }
            throw e;
        } finally {
            closeIfAnyIndexWriterHasTragedyOrIsClosed();
        }
    }

    private void closeIfAnyIndexWriterHasTragedyOrIsClosed() {
        if (writers.stream().map(writer -> writer.indexWriter).anyMatch(iw -> iw.getTragicException() != null || iw.isOpen() == false)) {
            try {
                close();
            } catch (Exception e) {
                logger.warn("failed to close persistent cache index", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            IOUtils.close(writers);
        }
    }

    /**
     * Creates a list of {@link CacheIndexWriter}, one for each data path of the specified {@link NodeEnvironment}.
     *
     * @param nodeEnvironment the data node environment
     * @return a list of {@link CacheIndexWriter}
     */
    private static List<CacheIndexWriter> createWriters(NodeEnvironment nodeEnvironment) {
        final List<CacheIndexWriter> writers = new ArrayList<>();
        boolean success = false;
        try {
            final NodeEnvironment.NodePath[] nodePaths = nodeEnvironment.nodePaths();
            for (NodeEnvironment.NodePath nodePath : nodePaths) {
                writers.add(createCacheIndexWriter(nodePath));
            }
            success = true;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create persistent cache writers", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writers);
            }
        }
        return unmodifiableList(writers);
    }

    /**
     * Creates a new {@link CacheIndexWriter} for the specified data path. The is a single instance per data path.
     *
     * @param nodePath the data path
     * @return a new {@link CacheIndexWriter} instance
     * @throws IOException if something went wrong
     */
    static CacheIndexWriter createCacheIndexWriter(NodeEnvironment.NodePath nodePath) throws IOException {
        final List<Closeable> closeables = new ArrayList<>();
        Path directoryPath = null;
        boolean success = false;
        try {
            directoryPath = createSnapshotCacheIndexFolder(nodePath);
            final Directory directory = FSDirectory.open(directoryPath);
            closeables.add(directory);

            final IndexWriterConfig config = new IndexWriterConfig(new KeywordAnalyzer());
            config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            config.setMergeScheduler(new SerialMergeScheduler());
            config.setRAMBufferSizeMB(1.0);
            config.setCommitOnClose(false);

            final IndexWriter indexWriter = new IndexWriter(directory, config);
            closeables.add(indexWriter);

            final CacheIndexWriter cacheIndexWriter = new CacheIndexWriter(nodePath, directoryPath, directory, indexWriter);
            success = true;
            return cacheIndexWriter;
        } finally {
            if (success == false) {
                IOUtils.close(closeables);
                if (directoryPath != null) {
                    IOUtils.rm(directoryPath);
                }
            }
        }
    }

    /**
     * Creates a list of {@link CacheIndexSearcher} for the specified data path. Each {@link CacheIndexSearcher} represents an existing
     * persistent cache Lucene index on disk and provides an {@link IndexSearcher} to search documents that contain information about
     * previously indexed {@link CacheFile}.
     *
     * @param nodePath the data path
     * @param ignorePath Lucene index path to ignore, usually the current {@link CacheIndexWriter} on the data path
     * @return a list of {@link CacheIndexSearcher}
     */
    private static List<CacheIndexSearcher> createSearchers(NodeEnvironment.NodePath nodePath, Path ignorePath) throws IOException {
        final List<CacheIndexSearcher> searchers = new ArrayList<>();
        boolean success = false;
        try {
            final Path snapshotCacheRootDir = resolveCacheIndexFolder(nodePath);
            if (FileSystemUtils.isAccessibleDirectory(snapshotCacheRootDir, logger) == false) {
                throw new IllegalStateException("Unable to access snapshot cache root directory at [" + snapshotCacheRootDir + ']');
            }
            try (DirectoryStream<Path> dirs = Files.newDirectoryStream(snapshotCacheRootDir, Files::isDirectory)) {
                for (Path dir : dirs) {
                    if (ignorePath != null && ignorePath.equals(dir) == false) {
                        searchers.add(createCacheIndexSearcher(dir));
                    }
                }
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(searchers);
            }
        }
        return unmodifiableList(searchers);
    }

    /**
     * Create a {@link CacheIndexSearcher} for the given directory path.
     *
     * @param directoryPath the Lucene index path
     * @return a {@link CacheIndexSearcher}
     * @throws IOException if an I/O exception occurs while creating Lucene Directory and IndexReader
     */
    static CacheIndexSearcher createCacheIndexSearcher(Path directoryPath) throws IOException {
        final List<Closeable> closeables = new ArrayList<>();
        boolean success = false;
        try {
            final Directory directory = FSDirectory.open(directoryPath);
            closeables.add(directory);

            final IndexReader indexReader = DirectoryReader.open(directory);
            closeables.add(indexReader);

            final IndexSearcher indexSearcher = new IndexSearcher(indexReader);
            indexSearcher.setQueryCache(null);

            final CacheIndexSearcher cacheIndexSearcher = new CacheIndexSearcher(directoryPath, directory, indexReader, indexSearcher);
            success = true;
            return cacheIndexSearcher;
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage("failed to open persistent cache index at [{}]", directoryPath), e);
            throw e; // TODO we could delete in case of corruption
        } finally {
            if (success == false) {
                IOUtils.close(closeables);
            }
        }
    }

    /**
     * Cleans any leftover searchable snapshot caches (files and Lucene indices) when a non-data node is starting up.
     * This is useful when the node is repurposed and is not a data node anymore.
     *
     * @param nodeEnvironment the {@link NodeEnvironment} to cleanup
     */
    public static void cleanUp(Settings settings, NodeEnvironment nodeEnvironment) {
        final boolean isDataNode = DiscoveryNode.isDataNode(settings);
        if (isDataNode) {
            assert false : "should not be called on data nodes";
            throw new IllegalStateException("Cannot clean searchable snapshot caches: node is a data node");
        }
        try {
            for (NodeEnvironment.NodePath nodePath : nodeEnvironment.nodePaths()) {
                for (String indexUUID : nodeEnvironment.availableIndexFoldersForPath(nodePath)) {
                    for (ShardId shardId : nodeEnvironment.findAllShardIds(new Index("_unknown_", indexUUID))) {
                        final Path shardDataPath = nodePath.resolve(shardId);
                        final ShardPath shardPath = new ShardPath(false, shardDataPath, shardDataPath, shardId);
                        final Path cacheDir = getShardCachePath(shardPath);
                        if (Files.isDirectory(cacheDir)) {
                            logger.debug("deleting searchable snapshot shard cache directory [{}]", cacheDir);
                            IOUtils.rm(cacheDir);
                        }
                    }
                }
                final Path cacheIndexDir = resolveCacheIndexFolder(nodePath);
                if (Files.isDirectory(cacheIndexDir)) {
                    logger.debug("deleting searchable snapshot lucene directory [{}]", cacheIndexDir);
                    IOUtils.rm(cacheIndexDir);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to clean up searchable snapshots cache", e);
        }
    }

    /**
     * A {@link CacheIndexWriter} contains a Lucene {@link Directory} with an {@link IndexWriter} that can be used to index documents in
     * the persistent cache index. There is one {@link CacheIndexWriter} for each data path.
     */
    static class CacheIndexWriter implements Closeable {

        private final NodeEnvironment.NodePath nodePath;
        private final IndexWriter indexWriter;
        private final Directory directory;
        private final Path path;

        private CacheIndexWriter(NodeEnvironment.NodePath nodePath, Path path, Directory directory, IndexWriter indexWriter) {
            this.path = path;
            this.nodePath = nodePath;
            this.directory = directory;
            this.indexWriter = indexWriter;
        }

        public Path directoryPath() {
            return path;
        }

        NodeEnvironment.NodePath nodePath() {
            return nodePath;
        }

        void updateCacheFile(CacheFile cacheFile, SortedSet<Tuple<Long, Long>> cacheRanges) throws IOException {
            final Term term = buildTerm(cacheFile);
            logger.debug("{} updating document with term [{}]", this, term);
            indexWriter.updateDocument(term, buildDocument(cacheFile, cacheRanges));
        }

        void updateCacheFile(String cacheFileId, Document cacheFileDocument) throws IOException {
            final Term term = buildTerm(cacheFileId);
            logger.debug("{} updating document with term [{}]", this, term);
            indexWriter.updateDocument(term, cacheFileDocument);
        }

        void deleteCacheFile(CacheFile cacheFile) throws IOException {
            final Term term = buildTerm(cacheFile);
            logger.debug("{} deleting document with term [{}]", this, term);
            indexWriter.deleteDocuments(term);
        }

        void prepareCommit() throws IOException {
            logger.debug("{} preparing commit", this);
            final Map<String, String> commitData = new HashMap<>(1);
            commitData.put(NODE_VERSION_COMMIT_KEY, Integer.toString(Version.CURRENT.id));
            indexWriter.setLiveCommitData(commitData.entrySet());
            indexWriter.prepareCommit();
        }

        void commit() throws IOException {
            logger.debug("{} committing", this);
            indexWriter.commit();
        }

        @Override
        public void close() throws IOException {
            logger.debug("{} closing", this);
            IOUtils.close(indexWriter, directory);
        }

        @Override
        public String toString() {
            return "[persistent cache][writer#" + path.getFileName().toString() + "]";
        }
    }

    /**
     * A {@link CacheIndexWriter} contains a Lucene {@link Directory} with an {@link IndexReader} that can be used to search documents in
     * a persistent cache index. There is one or more {@link CacheIndexSearcher} for each data path, and they are supposed to be deleted
     * as soon as the current persistent cache index writer has been correctly repopulated from these.
     */
    static class CacheIndexSearcher implements Closeable {

        private final IndexSearcher indexSearcher;
        private final IndexReader indexReader;
        private final AtomicBoolean closed;
        private final Directory directory;
        private final Path path;

        private CacheIndexSearcher(Path path, Directory directory, IndexReader indexReader, IndexSearcher indexSearcher) {
            this.path = path;
            this.directory = directory;
            this.indexReader = indexReader;
            this.indexSearcher = indexSearcher;
            this.closed = new AtomicBoolean();
        }

        public Path directoryPath() {
            return path;
        }

        IndexSearcher indexSearcher() {
            return indexSearcher;
        }

        @Nullable
        public Document find(Query query) throws IOException {
            final TopDocs docs = indexSearcher().search(query, 1);
            if (docs.totalHits.value == 1L) {
                return indexSearcher().doc(docs.scoreDocs[0].doc);
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                logger.debug("{} closing", this);
                IOUtils.close(indexReader, directory);
            }
        }

        @Override
        public String toString() {
            return "[persistent cache][searcher#" + path.getFileName().toString() + "]";
        }
    }

    /**
     * A {@link CacheFileVisitor} is used to visit cache files on disk and find information about them in the existing persistent cache
     * indices (using the given list of {@link CacheIndexSearcher}). If no cache file information are found, the file is deleted. If
     * information are found, the cache file is reindexed in the current persistent cache writer (using the given {@link CacheIndexWriter}
     * and inserted in the searchable snapshots cache (using the given {@link CacheService}).
     */
    private static class CacheFileVisitor extends SimpleFileVisitor<Path> {

        private final CacheIndexWriter writer;
        private final CacheService cacheService;
        private final List<CacheIndexSearcher> searchers;

        private CacheFileVisitor(CacheService cacheService, CacheIndexWriter writer, List<CacheIndexSearcher> searchers) {
            this.cacheService = Objects.requireNonNull(cacheService);
            this.searchers = Objects.requireNonNull(searchers);
            this.writer = Objects.requireNonNull(writer);
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            try {
                final String id = buildId(file);
                final Query query = new TermQuery(buildTerm(id));

                Document cacheDocument = null;
                for (CacheIndexSearcher searcher : searchers) {
                    cacheDocument = searcher.find(query);
                    if (cacheDocument != null) {
                        break;
                    }
                }
                if (cacheDocument != null) {
                    logger.trace("reindexing cache file with id [{}] in persistent cache index", id);
                    writer.updateCacheFile(id, cacheDocument);

                    final CacheKey cacheKey = getCacheKey(cacheDocument);
                    logger.trace("adding cache file with id [{}] and key [{}] in cache", id, cacheKey);
                    cacheService.put(cacheKey, getFileLength(cacheDocument), file.getParent(), id, getCacheFileRanges(cacheDocument));
                } else {
                    logger.trace("deleting cache file [{}], it does not exist in persistent cache index", file);
                    Files.delete(file);
                }
            } catch (Exception e) {
                throw ExceptionsHelper.convertToRuntime(e);
            }
            return FileVisitResult.CONTINUE;
        }
    }

    private static final String CACHE_ID_FIELD = "cache_id";
    private static final String CACHE_PATH_FIELD = "cache_path";
    private static final String CACHE_RANGES_FIELD = "cache_ranges";
    private static final String SNAPSHOT_ID_FIELD = "snapshot_id";
    private static final String SNAPSHOT_NAME_FIELD = "snapshot_name";
    private static final String INDEX_ID_FIELD = "index_id";
    private static final String INDEX_NAME_FIELD = "index_name";
    private static final String SHARD_INDEX_NAME_FIELD = "shard_index_name";
    private static final String SHARD_INDEX_ID_FIELD = "shard_index_id";
    private static final String SHARD_ID_FIELD = "shard_id";
    private static final String FILE_NAME_FIELD = "file_name";
    private static final String FILE_LENGTH_FIELD = "file_length";

    private static String buildId(CacheFile cacheFile) {
        return buildId(cacheFile.getFile());
    }

    private static String buildId(Path path) {
        return path.getFileName().toString();
    }

    private static Term buildTerm(CacheFile cacheFile) {
        return buildTerm(buildId(cacheFile));
    }

    private static Term buildTerm(String cacheFileUuid) {
        return new Term(CACHE_ID_FIELD, cacheFileUuid);
    }

    private static Document buildDocument(CacheFile cacheFile, SortedSet<Tuple<Long, Long>> cacheRanges) throws IOException {
        final Document document = new Document();
        document.add(new StringField(CACHE_ID_FIELD, buildId(cacheFile), Field.Store.NO));
        document.add(new StringField(CACHE_PATH_FIELD, cacheFile.getFile().toString(), Field.Store.YES));

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeVInt(cacheRanges.size());
            for (Tuple<Long, Long> cacheRange : cacheRanges) {
                output.writeVLong(cacheRange.v1());
                output.writeVLong(cacheRange.v2());
            }
            output.flush();
            document.add(new StoredField(CACHE_RANGES_FIELD, output.bytes().toBytesRef()));
        }

        final CacheKey cacheKey = cacheFile.getCacheKey();
        document.add(new StringField(FILE_NAME_FIELD, cacheKey.getFileName(), Field.Store.YES));
        document.add(new StringField(FILE_LENGTH_FIELD, Long.toString(cacheFile.getLength()), Field.Store.YES));

        final SnapshotId snapshotId = cacheKey.getSnapshotId();
        document.add(new StringField(SNAPSHOT_ID_FIELD, snapshotId.getUUID(), Field.Store.YES));
        document.add(new StringField(SNAPSHOT_NAME_FIELD, snapshotId.getName(), Field.Store.YES));

        final IndexId indexId = cacheKey.getIndexId();
        document.add(new StringField(INDEX_ID_FIELD, indexId.getName(), Field.Store.YES));
        document.add(new StringField(INDEX_NAME_FIELD, indexId.getId(), Field.Store.YES));

        final ShardId shardId = cacheKey.getShardId();
        document.add(new StringField(SHARD_INDEX_NAME_FIELD, shardId.getIndex().getName(), Field.Store.YES));
        document.add(new StringField(SHARD_INDEX_ID_FIELD, shardId.getIndex().getUUID(), Field.Store.YES));
        document.add(new StringField(SHARD_ID_FIELD, Integer.toString(shardId.getId()), Field.Store.YES));

        return document;
    }

    private static CacheKey getCacheKey(Document document) {
        return new CacheKey(
            new SnapshotId(document.get(SNAPSHOT_ID_FIELD), document.get(SNAPSHOT_NAME_FIELD)),
            new IndexId(document.get(INDEX_ID_FIELD), document.get(INDEX_NAME_FIELD)),
            new ShardId(
                new Index(document.get(SHARD_INDEX_NAME_FIELD), document.get(SHARD_INDEX_ID_FIELD)),
                Integer.parseInt(document.get(SHARD_ID_FIELD))
            ),
            document.get(FILE_NAME_FIELD)
        );
    }

    private static long getFileLength(Document document) {
        final String fileLength = document.get(FILE_LENGTH_FIELD);
        assert fileLength != null;
        return Long.parseLong(fileLength);
    }

    private static SortedSet<Tuple<Long, Long>> getCacheFileRanges(Document document) throws IOException {
        final SortedSet<Tuple<Long, Long>> cacheRanges = new TreeSet<>(Comparator.comparingLong(Tuple::v1));
        final BytesRef cacheRangesBytesRef = document.getBinaryValue(CACHE_RANGES_FIELD);
        try (StreamInput input = new ByteBufferStreamInput(ByteBuffer.wrap(cacheRangesBytesRef.bytes))) {
            final int length = input.readVInt();
            assert length > 0 : "empty cache ranges";
            Tuple<Long, Long> previous = null;
            for (int i = 0; i < length; i++) {
                final Tuple<Long, Long> range = Tuple.tuple(input.readVLong(), input.readVLong());
                assert previous == null || previous.v2() < range.v1();
                cacheRanges.add(range);

                /*
                for (Tuple<Long, Long> next : ranges) {
                    final Range range = new Range(next.v1(), next.v2(), null);
                    if (range.end <= range.start) {
                        throw new IllegalArgumentException("Range " + range + " cannot be empty");
                    }
                    if (length < range.end) {
                        throw new IllegalArgumentException("Range " + range + " is exceeding maximum length [" + length + ']');
                    }
                    if (previous != null && range.start <= previous.end) {
                        throw new IllegalArgumentException("Range " + range + " is overlapping a previous range " + previous);
                    }
                    final boolean added = this.ranges.add(range);
                    assert added : range + " already exist in " + this.ranges;
                    previous = range;
                }
                 */
            }
        }
        return unmodifiableSortedSet(cacheRanges);
    }

    static Path resolveCacheIndexFolder(NodeEnvironment.NodePath nodePath) {
        return CacheService.resolveSnapshotCache(nodePath.path);
    }

    /**
     * Creates a new directory (with a unique name) for the snapshot cache Lucene index.
     * <p>
     * This directory is located within a "snapshot_cache" directory at the root of the specified node data path.
     */
    private static Path createSnapshotCacheIndexFolder(NodeEnvironment.NodePath nodePath) throws IOException {
        // "snapshot_cache" directory at the root of the specified data path
        final Path snapshotCacheRootDir = resolveCacheIndexFolder(nodePath);
        if (Files.exists(snapshotCacheRootDir) == false) {
            logger.debug("creating new snapshot cache root directory [{}]", snapshotCacheRootDir);
            Files.createDirectories(snapshotCacheRootDir);
        }
        // create a new directory with a unique name in the "snapshot_cache" root directory
        for (int i = 0; i < 3; i++) {
            final Path snapshotCacheDir = snapshotCacheRootDir.resolve(UUIDs.randomBase64UUID());
            if (Files.exists(snapshotCacheDir) == false) {
                logger.debug("creating new snapshot cache directory [{}]", snapshotCacheDir);
                return Files.createDirectories(snapshotCacheDir);
            }
        }
        throw new IllegalStateException("Failed to create snapshot cache directory at [" + snapshotCacheRootDir + ']');
    }

}
