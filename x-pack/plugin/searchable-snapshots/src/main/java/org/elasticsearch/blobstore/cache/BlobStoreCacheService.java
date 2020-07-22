/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;

import java.io.IOException;
import java.time.Instant;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN;

public class BlobStoreCacheService {

    private static final Logger logger = LogManager.getLogger(BlobStoreCacheService.class);

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30L);

    private final ClusterService clusterService;
    protected final ThreadPool threadPool;
    private final Client client;
    private final String index;

    public BlobStoreCacheService(ClusterService clusterService, ThreadPool threadPool, Client client, String index) {
        this.client = new OriginSettingClient(client, SEARCHABLE_SNAPSHOTS_ORIGIN);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.index = index;
    }

    private void createIndexIfNecessary(ActionListener<String> listener) {
        if (clusterService.state().routingTable().hasIndex(index)) {
            listener.onResponse(index);
            return;
        }
        try {
            client.admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings())
                .setMapping(mappings())
                .execute(ActionListener.wrap(success -> listener.onResponse(index), e -> {
                    if (e instanceof ResourceAlreadyExistsException
                        || ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        listener.onResponse(index);
                    } else {
                        listener.onFailure(e);
                    }
                }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public CachedBlob get(String repository, String blobName, BlobPath blobPath, long offset) {
        final PlainActionFuture<CachedBlob> future = PlainActionFuture.newFuture();
        get(repository, blobName, blobPath, offset, future);
        return future.actionGet();
    }

    public void get(String repository, String blobName, BlobPath blobPath, long position, ActionListener<CachedBlob> listener) {
        assert assertCurrentThread();
        createIndexIfNecessary(ActionListener.wrap(indexName -> {
            try {
                final GetRequest request = new GetRequest(index).id(CachedBlob.computeId(repository, blobName, blobPath, position));
                client.get(request, ActionListener.wrap(response -> {
                    if (response.isExists()) {
                        assert response.isSourceEmpty() == false;
                        logger.trace("document [{}] found in cache", request);
                        listener.onResponse(CachedBlob.fromSource(response.getSource()));
                        // TODO trigger an update of last accessed time ?
                    } else {
                        logger.trace("document [{}] not found in cache", request);
                        listener.onResponse(null);
                    }
                }, e -> {
                    if (e instanceof IndexNotFoundException || e instanceof NoShardAvailableActionException) {
                        // Blob store cache system index might not be available at that time,
                        // so we pretend we didn't find a cache entry and we move on.
                        //
                        // Failing here would bubble up the exception and fail the searchable
                        // snapshot shard which is potentially recovering.
                        //
                        listener.onResponse(null);
                    } else {
                        listener.onFailure(e);
                    }
                }));
            } catch (Exception e) {
                listener.onFailure(e);
            }

        }, listener::onFailure));
    }

    public void put(String repository, String blobName, BlobPath blobPath, ReleasableBytesReference content, long offset) {
        createIndexIfNecessary(new ActionListener<>() {
            @Override
            public void onResponse(String indexName) {
                final Instant now = Instant.ofEpochMilli(threadPool.absoluteTimeInMillis());
                try {
                    final IndexRequest request = createIndexRequest(
                        new CachedBlob(now, now, Version.CURRENT, repository, blobName, blobPath, content, offset)
                    );
                    client.index(request, ActionListener.runAfter(ActionListener.wrap(response -> {
                        if (response.status() == RestStatus.CREATED) {
                            logger.trace("document [{}] successfully indexed in [{}]", request, indexName);
                        }
                    },
                        e -> logger.error(
                            () -> new ParameterizedMessage("failed to index document [{}] in [{}] system index", request, indexName),
                            e
                        )
                    ), () -> IOUtils.closeWhileHandlingException(content)));
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage("failed to index document in [{}] system index", indexName), e);
                    IOUtils.closeWhileHandlingException(content);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> new ParameterizedMessage("failed to create [{}] system index", index), e);
                IOUtils.closeWhileHandlingException(content);
            }
        });
    }

    private static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .put(IndexMetadata.SETTING_PRIORITY, "900")
            .build();
    }

    private static XContentBuilder mappings() throws IOException {
        final XContentBuilder builder = jsonBuilder();
        {
            builder.startObject();
            {
                builder.startObject(SINGLE_MAPPING_NAME);
                builder.field("dynamic", "false");
                {
                    builder.startObject("_meta");
                    builder.field("version", Version.CURRENT);
                    builder.endObject();
                }
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("type");
                        builder.field("type", "keyword");
                        builder.endObject();
                    }
                    {
                        builder.startObject("creation_time");
                        builder.field("type", "date");
                        builder.field("format", "epoch_millis");
                        builder.endObject();
                    }
                    {
                        builder.startObject("accessed_time");
                        builder.field("type", "date");
                        builder.field("format", "epoch_millis");
                        builder.endObject();
                    }
                    {
                        builder.startObject("version");
                        builder.field("type", "integer");
                        builder.endObject();
                    }
                    {
                        builder.startObject("repository");
                        builder.field("type", "keyword");
                        builder.endObject();
                    }
                    {
                        builder.startObject("blob");
                        builder.field("type", "object");
                        {
                            builder.startObject("properties");
                            {
                                builder.startObject("name");
                                builder.field("type", "keyword");
                                builder.endObject();
                                builder.startObject("path");
                                builder.field("type", "keyword");
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    {
                        builder.startObject("data");
                        builder.field("type", "object");
                        {
                            builder.startObject("properties");
                            {
                                builder.startObject("content");
                                builder.field("type", "binary");
                                builder.endObject();
                            }
                            {
                                builder.startObject("length");
                                builder.field("type", "long");
                                builder.endObject();
                            }
                            {
                                builder.startObject("offset");
                                builder.field("type", "long");
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }

    private IndexRequest createIndexRequest(CachedBlob cachedBlob) throws IOException {
        final IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.id(cachedBlob.id());
        try (XContentBuilder xContentBuilder = jsonBuilder()) {
            indexRequest.source(cachedBlob.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS));
        }
        return indexRequest;
    }

    private boolean assertCurrentThread() {
        final String threadName = Thread.currentThread().getName();
        assert threadName.contains('[' + ThreadPool.Names.GENERIC + ']')
            || threadName.contains('[' + ThreadPool.Names.SNAPSHOT + ']')
            || threadName.contains('[' + SearchableSnapshotsConstants.CACHE_FETCH_ASYNC_THREAD_POOL_NAME + ']') : "Unexpected thread name "
                + threadName;
        return true;
    }
}
