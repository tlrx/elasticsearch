/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indexing;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.RoutingFields;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SyntheticIdIT extends ESIntegTestCase {

    private record TsIdAndTimestamp(BytesRef tsId, long timestamp) {}

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    public void testSyntheticId() throws Exception {
        final var indexName = "synthetic-ids";
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
                    .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.DEFAULT_CODEC)
                    .put(IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.getKey(), true)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
                    .put(InternalSettingsPlugin.USE_COMPOUND_FILE.getKey(), false)
                    .put(IndexSettings.USE_SYNTHETIC_ID.getKey(), true)
                    .build()
            )
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "hostname",
                    "type=keyword,time_series_dimension=true",
                    "metric.field",
                    "type=keyword",
                    "metric.value",
                    "type=integer"
                )
        );

        final Instant timestamp = Instant.now();

        // Index 4 docs + 1 update
        var results = indexDocuments(
            indexName,
            document(timestamp, "vm-dev01", "cpu-load", 0),
            document(timestamp, "vm-dev02", "cpu-load", 1),
            document(timestamp.plusSeconds(2), "vm-dev02", "cpu-load", 3),
            document(timestamp.plusSeconds(5), "vm-dev03", "cpu-load", 4),
            document(timestamp.plusSeconds(2), "vm-dev02", "cpu-load", 5) // update
        );

        // Verify documents ids
        var id0 = parseSyntheticId(results[0].getId());
        assertThat(id0.timestamp, equalTo(timestamp.toEpochMilli()));
        assertThat(id0.tsId.bytesEquals(routingHash(indexName, "hostname", "vm-dev01")), equalTo(true));
        assertThat(results[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[0].getVersion(), equalTo(1L));

        var id1 = parseSyntheticId(results[1].getId());
        assertThat(id1.timestamp, equalTo(timestamp.toEpochMilli()));
        assertThat(id1.tsId.bytesEquals(routingHash(indexName, "hostname", "vm-dev02")), equalTo(true));
        assertThat(results[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[1].getVersion(), equalTo(1L));

        var id2 = parseSyntheticId(results[2].getId());
        assertThat(id2.timestamp, equalTo(timestamp.plusSeconds(2).toEpochMilli()));
        assertThat(id2.tsId.bytesEquals(routingHash(indexName, "hostname", "vm-dev02")), equalTo(true));
        assertThat(results[2].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[2].getVersion(), equalTo(1L));

        var id3 = parseSyntheticId(results[3].getId());
        assertThat(id3.timestamp, equalTo(timestamp.plusSeconds(5).toEpochMilli()));
        assertThat(id3.tsId.bytesEquals(routingHash(indexName, "hostname", "vm-dev03")), equalTo(true));
        assertThat(results[3].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(results[3].getVersion(), equalTo(1L));

        var id4 = parseSyntheticId(results[4].getId());
        assertThat(id4.timestamp, equalTo(timestamp.plusSeconds(2).toEpochMilli()));
        assertThat(id4.tsId.bytesEquals(routingHash(indexName, "hostname", "vm-dev02")), equalTo(true));
        assertThat(results[4].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        assertThat(results[4].getId(), equalTo(results[2].getId()));
        assertThat(results[4].getVersion(), equalTo(2L));

        // Get by synthetic _id
        var docId = results[4].getId();
        var getResponse = client().prepareGet(indexName, docId).setFetchSource(true).execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2L));
        assertThat(getResponse.getId(), equalTo(docId));
        var source = asInstanceOf(Map.class, getResponse.getSourceAsMap().get("metric"));
        assertThat(asInstanceOf(Integer.class, source.get("value")), equalTo(5));

        // Refresh
        assertHitCount(client().prepareSearch(indexName).setSize(0), 0L);
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0), 4L);

        // Update
        docId = results[0].getId();
        var updateResponse = client().prepareUpdate(indexName, docId)
            .setDoc(document(timestamp, "vm-dev01", "cpu-load", 6))
            .execute()
            .actionGet();
        assertThat(updateResponse.getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        assertThat(updateResponse.getVersion(), equalTo(2L));
        assertThat(updateResponse.getId(), equalTo(docId));

        // Delete by synthetic _id
        var deleteResponse = client().prepareDelete(indexName, docId).execute().actionGet();
        assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
        assertThat(deleteResponse.getVersion(), equalTo(3L));
        assertThat(deleteResponse.getId(), equalTo(docId));

        // Refresh
        assertHitCount(client().prepareSearch(indexName).setSize(0), 4L);
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0), 3L);

        // Doesnt' work:
        /*
        var searchResponse = client().prepareSearch(indexName)
            .setQuery(QueryBuilders.termQuery(IdFieldMapper.NAME, docId))
            .execute()
            .actionGet();
        */
    }

    private static BulkItemResponse[] indexDocuments(String indexName, XContentBuilder... docs) {
        final var client = client();
        assertThat(docs, notNullValue());
        var bulkRequest = client.prepareBulk();
        for (var source : docs) {
            bulkRequest.add(client.prepareIndex(indexName).setSource(source));
        }
        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);
        return bulkResponse.getItems();
    }

    private static XContentBuilder document(Instant timestamp, String hostName, String metricField, Integer metricValue)
        throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .field("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp))
            .field("hostname", hostName)
            .startObject("metric")
            .field("field", metricField)
            .field("value", metricValue)
            .endObject()
            .endObject();
    }

    private static TsIdAndTimestamp parseSyntheticId(String id) {
        var encoded = Uid.encodeId(id);
        // _id synthetic format = [timestamp(long), tsid]
        final byte[] idAsBytes = encoded.bytes;
        final var tsIdTerm = new BytesRefBuilder();
        tsIdTerm.copyBytes(idAsBytes, Long.BYTES, Math.toIntExact(idAsBytes.length - Long.BYTES));
        final long timestamp = ByteUtils.readLongBE(idAsBytes, 0);
        return new TsIdAndTimestamp(tsIdTerm.toBytesRef(), timestamp);
    }

    private BytesRef routingHash(String indexName, String fieldName, String fieldValue) {
        var indicesService = internalCluster().getInstance(
            IndicesService.class,
            internalCluster().nodesInclude(indexName).iterator().next()
        );
        var indexService = indicesService.indexService(resolveIndex(indexName));
        var routingPath = asInstanceOf(RoutingPathFields.class, RoutingFields.fromIndexSettings(indexService.getIndexSettings()));
        routingPath.addString(fieldName, fieldValue);
        return routingPath.buildHash().toBytesRef();
    }
}
