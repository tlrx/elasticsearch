package org.elasticsearch.index.engine;

import com.amazonaws.services.s3.internal.MD5DigestCalculatingInputStream;
import com.amazonaws.util.Base16;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.repositories.blobstore.BlobStoreDirectory;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.FrozenIndices;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class SearchableSnapshotsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(FrozenIndices.class, S3RepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        InetSocketAddress address = httpServer.getAddress();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.test.access_key", "access");
        secureSettings.setString("s3.client.test.secret_key", "secret");
        return Settings.builder()
            .put("s3.client.test.endpoint", "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort())
            .put(super.nodeSettings())
            .setSecureSettings(secureSettings)
            .build();
    }

    @TestLogging(value = "org.elasticsearch.repositories.blobstore:TRACE,org.elasticsearch.repositories.s3:TRACE", reason = "debug")
    public void testSearchableSnapshot() throws Exception {
        final String index = "test";
        createIndex(index, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());

        final long nbDocs = randomLongBetween(100_000L, 120_000L);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, "_doc", client(), (int) nbDocs)) {
            assertBusy(() -> assertThat(indexer.totalIndexedDocs(), greaterThanOrEqualTo(nbDocs)));
        }

        assertBusy(() -> assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs));

        ForceMergeResponse forceMerge = client().admin().indices().prepareForceMerge(index).setFlush(true).setMaxNumSegments(1).get();
        assertThat(forceMerge.getSuccessfulShards(), equalTo(1));
        assertHitCount(client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get(), nbDocs);

        final String repository = "repository";
        if (randomBoolean()) {
            logger.info("creating s3 repository {}", repository);
            assertAcked(client().admin().cluster().preparePutRepository(repository)
                .setType("s3")
                .setVerify(randomBoolean())
                .setSettings(Settings.builder()
                    .put("compress", randomBoolean())
                    .put("bucket", "bucket")
                    .put("client", "test")
                    .put("disable_chunked_encoding", true)
                    .put("chunk_size", new ByteSizeValue(5L, ByteSizeUnit.MB))
                    .put("buffer_size", new ByteSizeValue(5L, ByteSizeUnit.MB))
                    .build()));
        } else {
            logger.info("creating fs repository {}", repository);
            final Settings.Builder settings = Settings.builder();
            settings.put("compress", randomBoolean());
            settings.put("location", ESIntegTestCase.randomRepoPath(node().settings()));
            if (randomBoolean()) {
                long size = 1 << randomInt(10);
                //settings.put("chunk_size", new ByteSizeValue(size, ByteSizeUnit.KB));
                settings.put("chunk_size", new ByteSizeValue(5L, ByteSizeUnit.MB));
            }
            assertAcked(client().admin().cluster().preparePutRepository(repository)
                .setType("fs")
                .setVerify(randomBoolean())
                .setSettings(settings));
        }

        assertAcked(client().admin().indices().prepareClose(index));

        final String snapshot = "snapshot";
        assertThat(client().admin().cluster().prepareCreateSnapshot(repository, snapshot).setIndices(index).setWaitForCompletion(true)
            .get().getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        // Use the Freeze API to transit from open -> frozen + searchable snapshots
        //
        // POST docs/_freeze
        //{
        //  "index": {
        //    "glacial": {
        //      "repository": "repository",
        //      "snapshot": "snapshot-0",
        //      "index": "docs",
        //      "buffer_size": "1mb"
        //    }
        //  }
        //}
        //
        logger.info("freezing index with {} docs", nbDocs);
        final FreezeRequest freezeRequest = new FreezeRequest(index);
        freezeRequest.settings(Settings.builder()
            .put(BlobStoreDirectory.REPOSITORY_NAME.getKey(), repository)
            .put(BlobStoreDirectory.REPOSITORY_SNAPSHOT.getKey(), snapshot)
            .put(BlobStoreDirectory.REPOSITORY_INDEX.getKey(), index)
            .put(BlobStoreDirectory.REPOSITORY_BUFFER.getKey(),
                new ByteSizeValue(randomIntBetween(1, 10), randomFrom(ByteSizeUnit.KB, ByteSizeUnit.MB)))
            .build());
        assertAcked(client().execute(FreezeIndexAction.INSTANCE, freezeRequest).actionGet());
        ensureGreen(TimeValue.timeValueMinutes(10), index);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        for (IndexService indexService : indicesService) {
            if (indexService.getMetaData().getIndex().getName().equals(index)) {
                IndexShard indexShard = indexService.getShard(0);
                assertThat(FilterDirectory.unwrap(indexShard.store().directory()), instanceOf(BlobStoreDirectory.class));
                Path indexPath = indexShard.shardPath().resolveIndex();
                logger.info("removing {}", indexPath);
                IOUtils.rm(indexPath);
            }
        }

        assertHitCount(client().prepareSearch(index)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setTrackTotalHits(true)
            .setSize(0).get(), nbDocs);

        assertHitCount(client().prepareSearch(index)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setQuery(QueryBuilders.termQuery("test", "value5"))
            .setTrackTotalHits(true)
            .setSize(0).get(), 1);

        assertHitCount(client().prepareSearch(index)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setQuery(QueryBuilders.rangeQuery("id").lte(100))
            .setTrackTotalHits(true)
            .setSize(10).get(), 100L);

        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(index).get();
        assertThat(getSettingsResponse.getSetting(index, BlobStoreDirectory.REPOSITORY_NAME.getKey()), equalTo(repository));
        assertThat(getSettingsResponse.getSetting(index, BlobStoreDirectory.REPOSITORY_SNAPSHOT.getKey()), equalTo(snapshot));
        assertThat(getSettingsResponse.getSetting(index, BlobStoreDirectory.REPOSITORY_INDEX.getKey()), equalTo(index));
        assertThat(getSettingsResponse.getSetting(index, FrozenEngine.INDEX_FROZEN.getKey()), equalTo("true"));
    }

    private static HttpServer httpServer;
    private static ExecutorService executor;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        executor = Executors.newCachedThreadPool();
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 100);
        httpServer.setExecutor(executor);
        httpServer.start();
    }

    @Before
    public void setUpHttpServer() {
        httpServer.createContext("/bucket", new InternalHttpHandler());
    }

    @AfterClass
    public static void stopHttpServer() throws Exception {
        executor.shutdown();
        final boolean finishedNormally = executor.awaitTermination(15, TimeUnit.SECONDS);
        if (finishedNormally == false) {
            executor.shutdownNow();
        }
        httpServer.stop(0);
        httpServer = null;
    }


    private static final AtomicLong requests = new AtomicLong(0);

    /**
     * Minimal HTTP handler that acts as a S3 compliant server
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private static class InternalHttpHandler implements HttpHandler {

        private final ConcurrentMap<String, BytesReference> blobs = new ConcurrentHashMap<>();

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
            System.out.println("start " + requests.incrementAndGet() + " "
                + request + " " + exchange.getRequestHeaders().getFirst("Range"));
            try {
                if (Regex.simpleMatch("POST /bucket/*?uploads", request)) {
                    final String uploadId = UUIDs.randomBase64UUID();
                    byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<InitiateMultipartUploadResult>\n" +
                        "  <Bucket>bucket</Bucket>\n" +
                        "  <Key>" + exchange.getRequestURI().getPath() + "</Key>\n" +
                        "  <UploadId>" + uploadId + "</UploadId>\n" +
                        "</InitiateMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                    blobs.put(multipartKey(uploadId, 0), BytesArray.EMPTY);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else if (Regex.simpleMatch("PUT /bucket/*?uploadId=*&partNumber=*", request)) {
                    final Map<String, String> params = new HashMap<>();
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                    final String uploadId = params.get("uploadId");
                    if (blobs.containsKey(multipartKey(uploadId, 0))) {
                        final int partNumber = Integer.parseInt(params.get("partNumber"));
                        MD5DigestCalculatingInputStream md5 = new MD5DigestCalculatingInputStream(exchange.getRequestBody());
                        blobs.put(multipartKey(uploadId, partNumber), Streams.readFully(md5));
                        exchange.getResponseHeaders().add("ETag", Base16.encodeAsString(md5.getMd5Digest()));
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                    } else {
                        exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                    }

                } else if (Regex.simpleMatch("POST /bucket/*?uploadId=*", request)) {
                    Streams.readFully(exchange.getRequestBody());
                    final Map<String, String> params = new HashMap<>();
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                    final String uploadId = params.get("uploadId");

                    final int nbParts = blobs.keySet().stream()
                        .filter(blobName -> blobName.startsWith(uploadId))
                        .map(blobName -> blobName.replaceFirst(uploadId + '\n', ""))
                        .mapToInt(Integer::parseInt)
                        .max()
                        .orElse(0);

                    final ByteArrayOutputStream blob = new ByteArrayOutputStream();
                    for (int partNumber = 0; partNumber <= nbParts; partNumber++) {
                        BytesReference part = blobs.remove(multipartKey(uploadId, partNumber));
                        assertNotNull(part);
                        part.writeTo(blob);
                    }
                    blobs.put(exchange.getRequestURI().getPath(), new BytesArray(blob.toByteArray()));

                    byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<CompleteMultipartUploadResult>\n" +
                        "  <Bucket>bucket</Bucket>\n" +
                        "  <Key>" + exchange.getRequestURI().getPath() + "</Key>\n" +
                        "</CompleteMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                }else if (Regex.simpleMatch("PUT /bucket/*", request)) {
                    blobs.put(exchange.getRequestURI().toString(), Streams.readFully(exchange.getRequestBody()));
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);

                } else if (Regex.simpleMatch("GET /bucket/?prefix=*", request)) {
                    final Map<String, String> params = new HashMap<>();
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                    assertThat("Test must be adapted for GET Bucket (List Objects) Version 2", params.get("list-type"), nullValue());

                    final StringBuilder list = new StringBuilder();
                    list.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    list.append("<ListBucketResult>");
                    final String prefix = params.get("prefix");
                    if (prefix != null) {
                        list.append("<Prefix>").append(prefix).append("</Prefix>");
                    }
                    for (Map.Entry<String, BytesReference> blob : blobs.entrySet()) {
                        if (prefix == null || blob.getKey().startsWith("/bucket/" + prefix)) {
                            list.append("<Contents>");
                            list.append("<Key>").append(blob.getKey().replace("/bucket/", "")).append("</Key>");
                            list.append("<Size>").append(blob.getValue().length()).append("</Size>");
                            list.append("</Contents>");
                        }
                    }
                    list.append("</ListBucketResult>");

                    byte[] response = list.toString().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else if (Regex.simpleMatch("GET /bucket/*", request)) {
                    final BytesReference blob = blobs.get(exchange.getRequestURI().toString());
                    if (blob != null) {
                        final String range = exchange.getRequestHeaders().getFirst("Range");
                        if (range == null) {
                            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                            blob.writeTo(exchange.getResponseBody());
                        } else {
                            final Matcher matcher = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$").matcher(range);
                            assertTrue(matcher.matches());

                            final int start = Integer.parseInt(matcher.group(1));
                            final int end = Integer.parseInt(matcher.group(2));
                            final int length = end - start;

                            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                            exchange.getResponseHeaders().add("Content-Range",
                                String.format(Locale.ROOT, "bytes=%d-%d/%d", start, end, blob.length()));
                            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                            exchange.getResponseBody().write(BytesReference.toBytes(blob), start, length);
                        }
                    } else {
                        exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                    }

                } else if (Regex.simpleMatch("DELETE /bucket/*", request)) {
                    int deletions = 0;
                    for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<String, BytesReference> blob = iterator.next();
                        if (blob.getKey().startsWith(exchange.getRequestURI().toString())) {
                            iterator.remove();
                            deletions++;
                        }
                    }
                    exchange.sendResponseHeaders((deletions > 0 ? RestStatus.OK : RestStatus.NO_CONTENT).getStatus(), -1);

                } else if (Regex.simpleMatch("POST /bucket/?delete", request)) {
                    final String requestBody = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), UTF_8));

                    final StringBuilder deletes = new StringBuilder();
                    deletes.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                    deletes.append("<DeleteResult>");
                    for (Iterator<Map.Entry<String, BytesReference>> iterator = blobs.entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<String, BytesReference> blob = iterator.next();
                        String key = blob.getKey().replace("/bucket/", "");
                        if (requestBody.contains("<Key>" + key + "</Key>")) {
                            deletes.append("<Deleted><Key>").append(key).append("</Key></Deleted>");
                            iterator.remove();
                        }
                    }
                    deletes.append("</DeleteResult>");

                    byte[] response = deletes.toString().getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);

                } else {
                    exchange.sendResponseHeaders(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), -1);
                }
            } finally {
                exchange.close();
                System.out.println(" end " + requests.decrementAndGet() + " " +  request  + exchange.getRequestHeaders().getFirst("Range"));
            }
        }

        private static String multipartKey(final String uploadId, int partNumber) {
            return uploadId + "\n" + partNumber;
        }
    }
}
