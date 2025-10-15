/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.routing.RoutingHashBuilder;
import org.elasticsearch.common.lucene.SyntheticIdFieldsProducer;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper.syntheticIdField;

public class SyntheticIdPerfTests extends ESTestCase {

    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String TSID_FIELD = "_tsid";
    private static final long BASE_TIMESTAMP = 1704067200000L;

    public void testPerf() throws Exception {
        final boolean useSyntheticId = false;
        logger.info("indexing with synthetic id [" + useSyntheticId + ']');

        final boolean indexIdField = false; // unused when useSyntheticId is true
        logger.info("indexing _id field is [" + indexIdField + ']');

        var index = new Index("index", "_na_");
        var shardId = new ShardId(index, 0);
        var indexSettings = IndexSettingsModule.newIndexSettings(index.getName(), Settings.EMPTY);
        var dir = createTempDir().resolve(index.getUUID()).resolve(String.valueOf(shardId.id()));
        var shardPath = new ShardPath(false, dir, dir, shardId);
        Files.createDirectories(dir);

        try (var directory = new FsDirectoryFactory().newDirectory(indexSettings, shardPath)) {
            var indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            indexWriterConfig.setRAMBufferSizeMB(10.0);
            // NOTE: No merges
            indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
            // NOTE: index sort config matching TSDB sort order
            indexWriterConfig.setIndexSort(
                new Sort(
                    new SortField(TSID_FIELD, SortField.Type.STRING, false), // NOTE: hopefully this is correct?
                    new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)
                )
            );
            indexWriterConfig.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);

            if (useSyntheticId) {
                // Synthetic id codec
                indexWriterConfig.setCodec(new Elasticsearch92Lucene103Codec() {
                    @Override
                    public PostingsFormat getPostingsFormatForField(String field) {
                        // NOTE: This is the default posting format (ie, no bloom)
                        return new PostingsFormat("synthetic") {
                            @Override
                            public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                                assert false : "should not be called";
                                return null;
                            }

                            @Override
                            public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
                                DocValuesProducer docValuesProducer = null;
                                boolean success = false;
                                try {
                                    var codec = state.segmentInfo.getCodec();
                                    // Hack: The provided SegmentReadState uses the ES87BloomFilter suffix for filenames, while the tsids
                                    // won't have that. Just use an empty suffix to circumvent this for now.
                                    docValuesProducer = codec.docValuesFormat().fieldsProducer(new SegmentReadState(state, ""));
                                    var fieldsProducer = new SyntheticIdFieldsProducer(state, docValuesProducer);
                                    success = true;
                                    return fieldsProducer;
                                } finally {
                                    if (success == false) {
                                        IOUtils.close(docValuesProducer);
                                    }
                                }
                            }
                        };
                    }

                    @Override
                    public DocValuesFormat getDocValuesFormatForField(String field) {
                        // NOTE: Doc Values format for TSDB indices
                        return new ES819TSDBDocValuesFormat();
                    }
                });
            } else {
                // Default codec
                indexWriterConfig.setCodec(new Elasticsearch92Lucene103Codec() {
                    @Override
                    public PostingsFormat getPostingsFormatForField(String field) {
                        // NOTE: This is the default posting format (ie, no bloom)
                        return new Lucene103PostingsFormat();
                    }

                    @Override
                    public DocValuesFormat getDocValuesFormatForField(String field) {
                        // NOTE: Doc Values format for TSDB indices
                        return new ES819TSDBDocValuesFormat();
                    }
                });
            }

            try (var indexWriter = new IndexWriter(directory, indexWriterConfig)) {
                executeIndexing(indexWriter, useSyntheticId, indexIdField);
            }
        }
    }

    private void executeIndexing(final IndexWriter indexWriter, boolean useSyntheticId, boolean indexIdField) throws Exception {
        var startDate = Instant.ofEpochMilli(BASE_TIMESTAMP);
        var totalNumberOfHours = 24 * 10;
        var endDate = startDate.plus(totalNumberOfHours, HOURS);
        var metricIntervalInSeconds = 5;

        var indexTimeMetric = new MeanMetric();
        var threads = new ArrayList<Thread>();
        var numberOfIndexers = 32;
        final CountDown countDown = new CountDown(numberOfIndexers);
        for (int i = 0; i < numberOfIndexers; i++) {
            threads.add(
                new Thread(
                    new Indexer(
                        String.valueOf(i),
                        indexWriter,
                        useSyntheticId,
                        indexIdField,
                        indexTimeMetric,
                        startDate,
                        endDate,
                        metricIntervalInSeconds,
                        10,
                        countDown
                    )
                )
            );
        }

        logger.info("starting [" + numberOfIndexers + "] indexers");
        threads.forEach(Thread::start);

        var monitor = new Thread(() -> {
            while (countDown.isCountedDown() == false) {
                logger.info("--> average indexing time {}", indexTimeMetric.mean());
                safeSleep(1000);
            }
        });
        threads.add(monitor);
        monitor.start();

        for (Thread indexer : threads) {
            indexer.join();
        }
        logger.info("--> average indexing time {}", indexTimeMetric.mean());
        logger.info("indexers finished");
    }

    class Indexer implements Runnable {
        private final String id;
        private final IndexWriter indexWriter;
        private final boolean useSyntheticId;
        private final boolean indexIdField;
        private final MeanMetric metric;
        private final Instant endDate;
        private final long intervalInSeconds;
        private final int numberOfPodsInHost;
        private final CountDown countDown;
        private Instant currentTime;

        Indexer(
            String id,
            IndexWriter indexWriter,
            boolean useSyntheticId,
            boolean indexIdField,
            MeanMetric metric,
            Instant startDate,
            Instant endDate,
            long intervalInSeconds,
            int numberOfPodsInHost,
            CountDown countDown
        ) {
            this.id = id;
            this.indexWriter = indexWriter;
            this.useSyntheticId = useSyntheticId;
            this.indexIdField = indexIdField;
            this.metric = metric;
            this.endDate = endDate;
            this.currentTime = startDate;
            this.intervalInSeconds = intervalInSeconds;
            this.numberOfPodsInHost = numberOfPodsInHost;
            this.countDown = countDown;
        }

        @Override
        public void run() {
            try {
                while (currentTime.isBefore(endDate)) {
                    long timestamp = currentTime.toEpochMilli();
                    for (int i = 0; i < numberOfPodsInHost; i++) {
                        try {
                            // NOTE: default logic for computing _tsid/_id is extracted from
                            // org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper.postParse
                            var routingBuilder = new RoutingHashBuilder(s -> true);
                            final var routingPathFields = new RoutingPathFields(routingBuilder);
                            routingPathFields.addString("hostname field", new BytesRef("127.0." + id + '.' + i));
                            var tsid = routingPathFields.buildHash().toBytesRef();

                            var doc = new Document();
                            doc.add(new SortedDocValuesField(TSID_FIELD, tsid));
                            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp));
                            if (useSyntheticId) {
                                var id = TsidExtractingIdFieldMapper.createSyntheticId(tsid, timestamp);
                                BytesRef uidEncoded = Uid.encodeId(id);

                                doc.add(syntheticIdField(uidEncoded));
                            } else if (indexIdField) {
                                byte[] suffix = new byte[16];
                                var id = TsidExtractingIdFieldMapper.createId(false, routingBuilder, tsid, timestamp, suffix);
                                BytesRef uidEncoded = Uid.encodeId(id);

                                doc.add(new StringField(IdFieldMapper.NAME, uidEncoded, Field.Store.YES));
                            }

                            long startTime = System.nanoTime();
                            indexWriter.addDocuments(List.of(doc));
                            metric.inc(System.nanoTime() - startTime);
                        } catch (IOException e) {
                            logger.error("Failed to index Lucene document", e);
                            throw new AssertionError(e);
                        }
                    }
                    currentTime = currentTime.plusSeconds(intervalInSeconds);
                    // safeSleep(250);
                }
                logger.info("--> indexer finished");
            } finally {
                countDown.countDown();
            }
        }
    }
}
