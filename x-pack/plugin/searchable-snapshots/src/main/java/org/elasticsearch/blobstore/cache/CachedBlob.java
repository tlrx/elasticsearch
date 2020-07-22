/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.elasticsearch.Version;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

public class CachedBlob implements ToXContent {

    private static final String TYPE = "blob";

    private final Instant creationTime;
    private final Instant accessedTime;
    private final Version version;
    private final String repository;
    private final String name;
    private final BlobPath path;
    private final BytesReference bytes;
    private final long offset;
    private final String id;

    public CachedBlob(
        Instant creationTime,
        Instant accessedTime,
        Version version,
        String repository,
        String blobName,
        BlobPath blobPath,
        BytesReference blobContent,
        long offset,
        String id
    ) {
        this.creationTime = creationTime;
        this.accessedTime = accessedTime;
        this.version = version;
        this.repository = repository;
        this.name = blobName;
        this.path = blobPath;
        this.bytes = blobContent;
        this.offset = offset;
        this.id = id;
    }

    public CachedBlob(
        Instant creationTime,
        Instant accessedTime,
        Version version,
        String repository,
        String blobName,
        BlobPath blobPath,
        BytesReference blobContent,
        long offset
    ) {
        this(
            creationTime,
            accessedTime,
            version,
            repository,
            blobName,
            blobPath,
            blobContent,
            offset,
            computeId(repository, blobName, blobPath, offset)
        );
    }

    public String id() {
        return id;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return bytes.length();
    }

    public InputStream asInputStream() throws IOException {
        return bytes.streamInput();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("type", TYPE);
            builder.field("creation_time", creationTime.toEpochMilli());
            builder.field("accessed_time", accessedTime.toEpochMilli());
            builder.field("version", version.id);
            builder.field("repository", repository);
            builder.startObject("blob");
            {
                builder.field("name", name);
                builder.field("path", path.buildAsString());
            }
            builder.endObject();
            builder.startObject("data");
            {
                builder.field("content", BytesReference.toBytes(bytes));
                builder.field("length", bytes.length());
                builder.field("offset", offset);
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    @SuppressWarnings("unchecked")
    public static CachedBlob fromSource(final Map<String, Object> source) {
        final Long creationTimeEpochMillis = (Long) source.get("creation_time");
        if (creationTimeEpochMillis == null) {
            throw new IllegalStateException("cached blob document does not have the [creation_time] field");
        }
        final Long accessedTimeEpochMillis = (Long) source.get("accessed_time");
        if (accessedTimeEpochMillis == null) {
            throw new IllegalStateException("cached blob document does not have the [accessed_time] field");
        }
        final Version version = Version.fromId((Integer) source.get("version"));
        if (version == null) {
            throw new IllegalStateException("cached blob document does not have the [version] field");
        }
        final String repository = (String) source.get("repository");
        if (repository == null) {
            throw new IllegalStateException("cached blob document does not have the [repository] field");
        }

        final Map<String, ?> blob = (Map<String, ?>) source.get("blob");
        if (blob == null || blob.isEmpty()) {
            throw new IllegalStateException("cached blob document does not have the [blob] object");
        }
        final String name = (String) blob.get("name");
        if (name == null) {
            throw new IllegalStateException("cached blob document does not have the [blob.name] field");
        }
        final String path = (String) blob.get("path");
        if (path == null) {
            throw new IllegalStateException("cached blob document does not have the [blob.path] field");
        }

        final Map<String, ?> data = (Map<String, ?>) source.get("data");
        if (data == null || data.isEmpty()) {
            throw new IllegalStateException("cached blob document does not have the [data] fobjectield");
        }
        final String encodedContent = (String) data.get("content");
        if (encodedContent == null) {
            throw new IllegalStateException("cached blob document does not have the [data.content] field");
        }
        final Integer length = (Integer) data.get("length");
        if (length == null) {
            throw new IllegalStateException("cached blob document does not have the [data.length] field");
        }

        final byte[] content = Base64.getDecoder().decode(encodedContent);
        if (content.length != length) {
            throw new IllegalStateException("cached blob document content length does not match [data.length] field");
        }

        final Number offset = (Number) data.get("offset");
        if (offset == null) {
            throw new IllegalStateException("cached blob document does not have the [data.offset] field");
        }
        // TODO more verifications
        return new CachedBlob(
            Instant.ofEpochMilli(creationTimeEpochMillis),
            Instant.ofEpochMilli(accessedTimeEpochMillis),
            version,
            repository,
            name,
            new BlobPath(path),
            new BytesArray(content),
            offset.longValue()
        );
    }

    public static String computeId(String repository, String blobName, BlobPath blobPath, long offset) {
        BlobPath id = new BlobPath().add(repository);
        for (String path : blobPath.toArray()) {
            id = id.add(path);
        }
        id = id.add(blobName).add("@offset:" + offset);
        return id.buildAsString();
    }
}
