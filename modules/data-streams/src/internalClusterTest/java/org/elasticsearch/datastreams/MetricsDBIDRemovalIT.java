/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.IndexDocFailureStoreStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.codec.bloomfilter.BloomFilterSettings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

public class MetricsDBIDRemovalIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class);
    }

    private static final String TSDB_MAPPING = """
        {
           "_doc":{
              "properties":{
                 "@timestamp":{
                    "type":"date"
                 },
                 "metricset":{
                    "type":"keyword",
                    "time_series_dimension":true
                 },
                 "host": {
                    "type":"keyword",
                    "time_series_dimension":true
                 },
                 "k8s":{
                    "type":"object",
                    "properties":{
                       "pod":{
                          "type":"object",
                          "properties":{
                             "name":{
                                "type":"keyword",
                                "time_series_dimension":true
                             },
                             "uid":{
                                "type":"keyword"
                             },
                             "ip":{
                                "type":"ip"
                             },
                             "network":{
                                "type":"object",
                                "properties":{
                                   "tx":{
                                      "type":"long"
                                   },
                                   "rx":{
                                      "type":"long"
                                   }
                                }
                             }
                          }
                       }
                    }
                 }
              }
           }
        }""";

    private static final String TSDB_DOC = """
        {
            "@timestamp": "$time",
            "metricset": "$metricset",
            "host": "$host",
            "k8s": {
                "pod": {
                    "name": "$pod",
                    "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9",
                    "ip": "10.10.55.3",
                    "network": {
                        "tx": 1434595272,
                        "rx": 530605511
                    }
                }
            }
        }
        """;

    public void testSimpleScenario() throws Exception {
        var templateSettings = Settings.builder()
            .put("index.mode", "time_series")
            .put("index.routing_path", "metricset")
            .put("index.number_of_replicas", 0);
        var mapping = new CompressedXContent(TSDB_MAPPING);

        // create template
        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("k8s*"))
                .template(new Template(templateSettings.build(), mapping, null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        safeGet(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));

        // index doc
        Instant time = Instant.now();

        List<String> ids = new ArrayList<>();
        List<String> index = new ArrayList<>();
        var initialTime = time;
        for (int i = 0; i < 5; i++) {
            String replacement = formatInstant(time);
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(TSDB_DOC.replace("$time", replacement).replace("$metricset", "pod"), XContentType.JSON);
            var indexResponse = safeGet(client().index(indexRequest));
            ids.add(indexResponse.getId());
            index.add(indexResponse.getIndex());
            safeSleep(50);
            time = Instant.now();
        }
        for (int i = 0; i < 2; i++) {
            String replacement = formatInstant(time);
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(TSDB_DOC.replace("$time", replacement).replace("$metricset", "pod2"), XContentType.JSON);
            var indexResponse = safeGet(client().index(indexRequest));
            ids.add(indexResponse.getId());
            safeSleep(50);
            time = Instant.now();
        }

        refresh("k8s");

        var result = client().prepareGet(index.get(0), ids.get(0)).execute().actionGet();
        var source = result.getSourceAsString();

        // Delete breaks on refresh because it tries to find terms for the _id field, but we're returning an empty terms
        // and that would break while it tries to compute the live docs after a refresh.
        // See FrozenBufferedUpdates.applyDocValuesUpdates

        // client().prepareDelete(index.get(0), ids.get(0)).execute().actionGet();
        // refresh("k8s");

        assertNoFailuresAndResponse(
            client().prepareSearch("k8s").setFetchSource(true).setQuery(matchAllQuery()).execute(),
            searchResponse -> {
                for (SearchHit hit : searchResponse.getHits()) {
                    logger.info("hit: {} {}", hit.getId(), hit.getSourceAsString());
                }
            }
        );

        for (int i = 0; i < 2; i++) {
            String replacement = formatInstant(time);
            var indexRequest = new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(TSDB_DOC.replace("$time", replacement).replace("$metricset", "pod3"), XContentType.JSON);
            var indexResponse = safeGet(client().index(indexRequest));
            safeSleep(150);
            time = Instant.now();
        }

        refresh("k8s");

        forceMerge();

        Throwable indexExceptionCause = expectThrows(
            IndexDocFailureStoreStatus.ExceptionWithFailureStoreStatus.class,
            () -> client().index(
                new IndexRequest("k8s").opType(DocWriteRequest.OpType.CREATE)
                    .source(TSDB_DOC.replace("$time", formatInstant(initialTime)).replace("$metricset", "pod"), XContentType.JSON)
            ).actionGet()
        ).getCause();
        assertThat(indexExceptionCause.getClass(), equalTo(VersionConflictEngineException.class));
    }

    public void testPerf() throws Exception {
        BloomFilterSettings.SKIP_LOOKUP.set(false);
        BloomFilterSettings.INDEX_ID.set(true);
        BloomFilterSettings.FORCE_SEGMENT_LOOKUP.set(true);
        BloomFilterSettings.FORCE_LOOKUP.set(true);
        var templateSettings = Settings.builder().put("index.mode", "time_series").put("index.number_of_replicas", 0);
        var mapping = new CompressedXContent(TSDB_MAPPING);

        // create template
        var request = new TransportPutComposableIndexTemplateAction.Request("id");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("k8s*"))
                .template(new Template(templateSettings.build(), mapping, null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        safeGet(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));

        var startDate = Instant.now();
        var totalNumberOfHours = 3;
        var endDate = startDate.plus(totalNumberOfHours, HOURS);
        var metricIntervalInSeconds = 5;

        var indexers = new ArrayList<Thread>();
        var numberOfIndexers = 32;
        for (int i = 0; i < numberOfIndexers; i++) {
            indexers.add(new Thread(new Indexer("host-" + i, startDate, endDate, metricIntervalInSeconds, "prod", 10)));
        }

        logger.info("starting indexers");
        indexers.forEach(Thread::start);

        // Give enough time to the first requests to create the index
        safeSleep(200);
        var writeIndex = client().admin()
            .cluster()
            .prepareState(TimeValue.THIRTY_SECONDS)
            .get()
            .getState()
            .metadata()
            .getProject(ProjectId.DEFAULT)
            .dataStreams()
            .get("k8s")
            .getWriteIndex();
        IndexShard indexShard = null;
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            indexShard = indicesService.getShardOrNull(new ShardId(writeIndex, 0));
            if (indexShard != null) {
                break;
            }
        }
        assert indexShard != null;

        int reportAverageIndexOpTimeSeconds = 120;
        for (int i = 0; i < reportAverageIndexOpTimeSeconds; i++) {
            var indexingTimeMetric = indexShard.indexingTimeMetric();
            logger.info("--> average indexing time {}", indexingTimeMetric.mean());
            safeSleep(1000);
        }

        for (Thread indexer : indexers) {
            indexer.join();
        }
        logger.info("indexers finished");
    }

    class Indexer implements Runnable {
        private final String id;
        private final Instant endDate;
        private final long intervalInSeconds;
        private final String metricset;
        private final int numberOfPodsInHost;
        private Instant currentTime;

        Indexer(String id, Instant startDate, Instant endDate, long intervalInSeconds, String metricset, int numberOfPodsInHost) {
            this.id = id;
            this.endDate = endDate;
            this.currentTime = startDate;
            this.intervalInSeconds = intervalInSeconds;
            this.metricset = metricset;
            this.numberOfPodsInHost = numberOfPodsInHost;
        }

        @Override
        public void run() {
            while (currentTime.isBefore(endDate)) {
                var bulk = client().prepareBulk();
                String timestamp = formatInstant(currentTime);
                String doc = TSDB_DOC.replace("$time", timestamp).replace("$metricset", metricset).replace("$host", id);
                for (int i = 0; i < numberOfPodsInHost; i++) {
                    bulk.add(
                        client().prepareIndex()
                            .setIndex("k8s")
                            .setOpType(DocWriteRequest.OpType.CREATE)
                            .setSource(doc.replace("$pod", "pod-" + i), XContentType.JSON)
                    );
                }
                var response = bulk.get();
                if (response.hasFailures()) {
                    logger.warn("--> Bulk failure {}", response.buildFailureMessage());
                }
                currentTime = currentTime.plusSeconds(intervalInSeconds);
                safeSleep(250);
            }
        }
    }

    private static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }
}
