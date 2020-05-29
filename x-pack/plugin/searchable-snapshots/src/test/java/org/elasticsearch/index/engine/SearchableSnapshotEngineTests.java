/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.shard.DocsStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.function.BiConsumer;

import static java.util.Collections.synchronizedList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class SearchableSnapshotEngineTests extends EngineTestCase {

    public void testSearchConcurrently() throws Exception {
        int numDocs = 0;
        try (InternalEngine internalEngine = engine) {
            for (int i = 0; i < between(0, 1_000); i++) {
                final String docId = Integer.toString(i);
                internalEngine.index(indexForDoc(createParsedDoc(docId, null)));
                if (rarely()) {
                    internalEngine.flush();
                }
                if (randomBoolean()) {
                    Engine.DeleteResult result = internalEngine.delete(new Engine.Delete(docId, newUid(docId), primaryTerm.get()));
                    assertTrue(result.isFound());
                } else {
                    numDocs += 1;
                }
            }
            internalEngine.flush();
        }

        final int numDocsAdded = numDocs;
        try (SearchableSnapshotEngine snapshotEngine = new SearchableSnapshotEngine(engine.engineConfig)) {
            final Thread[] threads = new Thread[randomIntBetween(2, 5)];
            final int iterations = between(100, 1_000);
            final List<Exception> failures = synchronizedList(new ArrayList<>());

            final CyclicBarrier barrier = new CyclicBarrier(threads.length);
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try (Engine.Searcher searcher = snapshotEngine.acquireSearcher("search")) {
                        barrier.await();
                        for (int j = 0; j < iterations; j++) {
                            switch (randomInt(1)) {
                                case 0:
                                    final TotalHitCountCollector collector = new TotalHitCountCollector();
                                    searcher.search(new MatchAllDocsQuery(), collector);
                                    assertThat(collector.getTotalHits(), equalTo(numDocsAdded));
                                    break;
                                case 1:
                                    final TopDocs search = searcher.search(new MatchAllDocsQuery(), Math.min(10, numDocsAdded));
                                    assertEquals(search.scoreDocs.length, Math.min(10, numDocsAdded));
                                    break;
                                default:
                                    throw new AssertionError("Unsupported search test");
                            }
                        }
                    } catch (Exception e) {
                        failures.add(e);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            assertThat(failures, empty());
        }
    }

    public void testSegmentStats() throws IOException {
        final BiConsumer<SegmentsStats, SegmentsStats> verify = (expected, actual) -> {
            assertNotSame(expected, actual);
            assertEquals(expected.getCount(), actual.getCount());
            assertEquals(expected.getMemoryInBytes(), actual.getMemoryInBytes());
            assertEquals(expected.getTermsMemoryInBytes(), actual.getTermsMemoryInBytes());
            assertEquals(expected.getStoredFieldsMemoryInBytes(), actual.getStoredFieldsMemoryInBytes());
            assertEquals(expected.getTermVectorsMemoryInBytes(), actual.getTermVectorsMemoryInBytes());
            assertEquals(expected.getNormsMemoryInBytes(), actual.getNormsMemoryInBytes());
            assertEquals(expected.getPointsMemoryInBytes(), actual.getPointsMemoryInBytes());
            assertEquals(expected.getDocValuesMemoryInBytes(), actual.getDocValuesMemoryInBytes());
            assertEquals(expected.getIndexWriterMemoryInBytes(), actual.getIndexWriterMemoryInBytes());
            assertEquals(expected.getBitsetMemoryInBytes(), actual.getBitsetMemoryInBytes());
            assertEquals(expected.getFileSizes().size(), actual.getFileSizes().size());
            for (ObjectObjectCursor<String, Long> cursor : expected.getFileSizes()) {
                assertEquals(cursor.value, actual.getFileSizes().get(cursor.key));
            }
        };

        executeStatsTestCase(
            e -> e.segmentsStats(true, true),
            (expected, before) -> verify.accept(new SegmentsStats(), before),
            (expected, after) -> verify.accept(expected, after)
        );
    }

    public void testDocsStats() throws IOException {
        final BiConsumer<DocsStats, DocsStats> verify = (expected, actual) -> {
            assertNotSame(expected, actual);
            assertEquals(expected.getCount(), actual.getCount());
            assertEquals(expected.getDeleted(), actual.getDeleted());
            assertEquals(expected.getAverageSizeInBytes(), actual.getAverageSizeInBytes());
            assertEquals(expected.getTotalSizeInBytes(), actual.getTotalSizeInBytes());
        };

        executeStatsTestCase(
            e -> e.docStats(),
            (expected, before) -> verify.accept(new DocsStats(), before),
            (expected, after) -> verify.accept(expected, after)
        );
    }

    private <T> void executeStatsTestCase(
        CheckedFunction<Engine, T, IOException> statsSupplier,
        BiConsumer<T, T> before,
        BiConsumer<T, T> after
    ) throws IOException {
        int numDocs = 0;
        final T expected;
        try (InternalEngine internalEngine = engine) {
            for (int i = 0; i < between(0, 1_000); i++) {
                final String docId = Integer.toString(i);
                internalEngine.index(indexForDoc(createParsedDoc(docId, null)));
                if (rarely()) {
                    internalEngine.flush();
                }
                if (rarely()) {
                    Engine.DeleteResult result = internalEngine.delete(new Engine.Delete(docId, newUid(docId), primaryTerm.get()));
                    assertTrue(result.isFound());
                } else {
                    numDocs += 1;
                }
            }
            internalEngine.flush();
            expected = statsSupplier.apply(internalEngine);
        }

        try (SearchableSnapshotEngine snapshotEngine = new SearchableSnapshotEngine(engine.engineConfig)) {
            final T beforeRefreshStats = statsSupplier.apply(snapshotEngine);
            before.accept(expected, beforeRefreshStats);

            try (Engine.Searcher searcher = snapshotEngine.acquireSearcher("before")) {
                final DirectoryReader directoryReader = searcher.getDirectoryReader();
                assertThat(directoryReader, instanceOf(ElasticsearchDirectoryReader.class));
                assertThat(
                    ElasticsearchDirectoryReader.unwrap(directoryReader),
                    instanceOf(SearchableSnapshotEngine.LazyDirectoryReader.class)
                );
            }

            if (randomBoolean()) {
                snapshotEngine.refresh("refresh_needed");
            } else {
                try (Engine.Searcher searcher = snapshotEngine.acquireSearcher("search")) {
                    searcher.search(new MatchAllDocsQuery(), new TotalHitCountCollector());
                }
            }
            assertVisibleCount(snapshotEngine, numDocs);

            try (Engine.Searcher searcher = snapshotEngine.acquireSearcher("after")) {
                final DirectoryReader directoryReader = searcher.getDirectoryReader();
                assertThat(directoryReader, instanceOf(ElasticsearchDirectoryReader.class));
                assertThat(
                    ElasticsearchDirectoryReader.unwrap(directoryReader),
                    not(instanceOf(SearchableSnapshotEngine.LazyDirectoryReader.class))
                );
            }

            final T afterRefreshStats = statsSupplier.apply(snapshotEngine);
            after.accept(expected, afterRefreshStats);
        }
    }
}
