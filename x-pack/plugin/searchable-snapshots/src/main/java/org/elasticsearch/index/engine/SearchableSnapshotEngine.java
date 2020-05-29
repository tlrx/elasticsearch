/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class SearchableSnapshotEngine extends ReadOnlyEngine {

    public SearchableSnapshotEngine(EngineConfig config) {
        super(config, null, new TranslogStats(), true, Function.identity(), false); // never require complete history
    }

    @Override
    protected DirectoryReader open(IndexCommit commit) throws IOException {
        // Defers the opening of the directory reader until doOpenIfChanged() is called on the LazyDirectoryReader,
        // which happens when the engine's reference manager is refreshed.
        return new LazyDirectoryReader(commit, c -> wrapReader(super.open(c), Function.identity()));
    }

    private boolean isRefreshNeeded(String source) {
        return "refresh_needed".equals(source);
    }

    @Override
    public void refresh(String source) {
        if (isRefreshNeeded(source)) {
            assert Thread.currentThread().getName().contains(SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOTS_THREAD_POOL_NAME)
                || Thread.currentThread().getName().startsWith("TEST-")
                || Thread.currentThread().getName().startsWith("LuceneTestCase") : "current thread ["
                    + Thread.currentThread()
                    + "] may not refresh with source "
                    + source;
            try {
                if (store.tryIncRef()) {
                    try (Searcher searcher = acquireSearcher(source, SearcherScope.EXTERNAL)) {
                        assert assertDirectoryReaderRefreshed(searcher.getDirectoryReader());
                    } finally {
                        store.decRef();
                    }
                }
            } catch (Exception e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "failed to acquire searcher [source={}] in order to refresh searchable snapshot directory reader",
                        source
                    ),
                    e
                );
            }
        }
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException {
        if (store.tryIncRef() == false) {
            throw new AlreadyClosedException(shardId + " store is closed", failedEngine.get());
        }
        boolean success = false;
        try {
            // Refreshes the reference manager when a refresh is explicitly needed
            // or when a search is about to be executed, whichever comes first.
            final boolean shouldRefresh = "search".equals(source) || isRefreshNeeded(source);

            final ReferenceManager<ElasticsearchDirectoryReader> referenceManager = getReferenceManager(scope);
            if (shouldRefresh) {
                referenceManager.maybeRefreshBlocking();
            }
            final ElasticsearchDirectoryReader directoryReader = referenceManager.acquire();
            assert shouldRefresh == false || assertDirectoryReaderRefreshed(directoryReader);
            final AtomicBoolean released = new AtomicBoolean(false);
            final Searcher searcher = new Searcher(
                source,
                directoryReader,
                engineConfig.getSimilarity(),
                engineConfig.getQueryCache(),
                engineConfig.getQueryCachingPolicy(),
                () -> {
                    if (released.compareAndSet(false, true)) {
                        try {
                            referenceManager.release(directoryReader);
                        } finally {
                            store.decRef();
                        }
                    }
                }
            );
            success = true;
            return searcher;
        } catch (AlreadyClosedException e) {
            throw e;
        } catch (Exception e) {
            maybeFailEngine("acquire_searcher", e);
            ensureOpen(e); // throw EngineCloseException here if we are already closed
            logger.error(() -> new ParameterizedMessage("failed to acquire searchable snapshot searcher [source={}]", source), e);
            throw new EngineException(shardId, "failed to acquire searchable snapshot searcher [source={}]", source, e);
        } finally {
            if (success == false) {
                store.decRef();
            }
        }
    }

    private boolean assertDirectoryReaderRefreshed(DirectoryReader directoryReader) throws IOException {
        assert directoryReader.isCurrent();
        assert directoryReader instanceof ElasticsearchDirectoryReader;
        assert ElasticsearchDirectoryReader.unwrap(directoryReader) instanceof LazyDirectoryReader == false;
        return true;
    }

    static final class LazyDirectoryReader extends DirectoryReader {

        private final CheckedFunction<IndexCommit, ElasticsearchDirectoryReader, IOException> loader;
        private final AtomicBoolean initialized;
        private final IndexCommit commit;

        protected LazyDirectoryReader(IndexCommit commit, CheckedFunction<IndexCommit, ElasticsearchDirectoryReader, IOException> loader)
            throws IOException {
            super(commit.getDirectory(), new LeafReader[0]);
            this.initialized = new AtomicBoolean(false);
            this.commit = Objects.requireNonNull(commit);
            this.loader = loader;
        }

        @Override
        protected DirectoryReader doOpenIfChanged() throws IOException {
            if (initialized.compareAndSet(false, true)) {
                return loader.apply(commit);
            }
            assert false : "this method should not be called twice";
            return null;
        }

        @Override
        public boolean isCurrent() throws IOException {
            return true;
        }

        @Override
        public IndexCommit getIndexCommit() throws IOException {
            return commit;
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getVersion() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doClose() throws IOException {}

        @Override
        public CacheHelper getReaderCacheHelper() {
            throw new UnsupportedOperationException();
        }
    }
}
