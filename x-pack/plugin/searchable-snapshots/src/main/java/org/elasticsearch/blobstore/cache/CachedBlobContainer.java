/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.blobstore.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.PagedBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class CachedBlobContainer extends FilterBlobContainer {

    protected static final int DEFAULT_BYTE_ARRAY_SIZE = 1 << 14;
    private static final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

    private final Supplier<BlobStoreIndexShardSnapshot> snapshot;
    private final BlobStoreCacheService blobCacheService;
    private final String repository;

    public CachedBlobContainer(
        String repository,
        Supplier<BlobStoreIndexShardSnapshot> snapshot,
        BlobStoreCacheService blobStoreCacheService,
        BlobContainer delegate
    ) {
        super(delegate);
        this.repository = repository;
        this.snapshot = snapshot;
        this.blobCacheService = blobStoreCacheService;
    }

    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        return new CachedBlobContainer(repository, snapshot, blobCacheService, child);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        if (snapshot.get() == null) {
            // when file details are unknown for this shard snapshot,
            // only snap-%s.dat are allowed to be cached
            if (blobName.startsWith("snap-") && blobName.endsWith(".dat")) {
                final CachedBlob cachedBlob = blobCacheService.get(repository, blobName, path(), 0L);
                if (cachedBlob != null) {
                    return cachedBlob.asInputStream();
                }
                return new CopyOnReadInputStream(
                    super.readBlob(blobName),
                    ActionListener.wrap(
                        release -> blobCacheService.put(repository, blobName, path(), release, 0L),
                        e -> {}
                    )
                );
            }
        }
        assert false : "not supposed to read any other file from this method";
        return super.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        final BlobStoreIndexShardSnapshot.FileInfo blobInfo = fileInfo(blobName);
        assert blobInfo != null;

        if (isFullyCached(blobInfo)) {
            final InputStream stream = readFullyCachedBlob(blobInfo, blobName, position, length);
            if (stream != null) {
                return stream;
            }
        } else if (isFirstPartCached(blobInfo, blobName, position)) {
            final InputStream stream = readFirstPartCachedBlob(blobInfo, blobName, position, length);
            if (stream != null) {
                return stream;
            }
        } else if (isLastPartCached(blobInfo, blobName, position, length)) {
            final InputStream stream = readLastPartCachedBlob(blobInfo, blobName, position, length);
            if (stream != null) {
                return stream;
            }
        }
        return super.readBlob(blobName, position, length);
    }

    private BlobStoreIndexShardSnapshot.FileInfo fileInfo(String blobName) {
        final BlobStoreIndexShardSnapshot snapshot = this.snapshot.get();
        if (snapshot != null) {
            for (BlobStoreIndexShardSnapshot.FileInfo indexFile : snapshot.indexFiles()) {
                for (int i = 0; i < indexFile.numberOfParts(); i++) {
                    if (blobName.equals(indexFile.partName(i))) {
                        return indexFile;
                    }
                }
            }
        }
        return null;
    }

    private boolean isFullyCached(BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
        if (fileInfo.numberOfParts() == 1L) {
            return fileInfo.length() <= DEFAULT_BYTE_ARRAY_SIZE;
        }
        return false;
    }

    private InputStream readFullyCachedBlob(BlobStoreIndexShardSnapshot.FileInfo blobInfo, String blobName, long position, long length)
        throws IOException {
        final CachedBlob cachedBlob = blobCacheService.get(repository, blobName, path(), 0L);
        if (cachedBlob != null) {
            final InputStream stream = cachedBlob.asInputStream();
            if (0L < position) {
                stream.skip(position);
            }
            return Streams.limitStream(stream, length);
        }
        if (position == 0L && length == blobInfo.length()) {
            return new CopyOnReadInputStream(
                delegate.readBlob(blobName, 0L, blobInfo.length()),
                ActionListener.wrap(
                    release -> blobCacheService.put(repository, blobName, path(), release, 0L), e -> {}
                )
            );
        }
        return null;
    }

    private boolean isFirstPartCached(BlobStoreIndexShardSnapshot.FileInfo blobInfo, String blobName, long position) {
        if (blobName.equals(blobInfo.partName(0L))) {
            return position < DEFAULT_BYTE_ARRAY_SIZE;
        }
        return false;
    }

    private InputStream readFirstPartCachedBlob(BlobStoreIndexShardSnapshot.FileInfo blobInfo, String blobName, long position, long length)
        throws IOException {
        final CachedBlob cachedBlob = blobCacheService.get(repository, blobName, path(), 0L);
        if (cachedBlob != null) {
            final InputStream stream = cachedBlob.asInputStream();
            if (0L < position) {
                stream.skip(position);
            }
            if (length <= cachedBlob.length()) {
                return Streams.limitStream(stream, length);
            } else {
                return new SequenceInputStream(stream, delegate.readBlob(blobName, cachedBlob.length(), length - cachedBlob.length()));
            }
        }
        if (position == 0L && DEFAULT_BYTE_ARRAY_SIZE <= length) {
            return new CopyOnReadInputStream(
                delegate.readBlob(blobName, 0L, length),
                ActionListener.wrap(
                    release -> blobCacheService.put(repository, blobName, path(), release, 0L),
                    e -> {}
                )
            );
        }
        return null;
    }

    private boolean isLastPartCached(BlobStoreIndexShardSnapshot.FileInfo blobInfo, String blobName, long position, long length) {
        if (blobName.equals(blobInfo.partName(blobInfo.numberOfParts() - 1L))) {
            return (blobInfo.length() - position) <= DEFAULT_BYTE_ARRAY_SIZE;
        }
        return false;
    }

    private InputStream readLastPartCachedBlob(BlobStoreIndexShardSnapshot.FileInfo blobInfo, String blobName, long position, long length)
        throws IOException {
        final long lastOffset = blobInfo.length() - DEFAULT_BYTE_ARRAY_SIZE;
        assert lastOffset >= 0L;
        final CachedBlob cachedBlob = blobCacheService.get(repository, blobName, path(), lastOffset);
        if (cachedBlob != null) {
            final InputStream stream = cachedBlob.asInputStream();
            if (cachedBlob.offset() <= position) {
                stream.skip(position - cachedBlob.offset());
                return Streams.limitStream(stream, length);
            }
            return new SequenceInputStream(delegate.readBlob(blobName, position, cachedBlob.offset() - position), stream);
        }
        if (position + length == blobInfo.length()) {
            final InputStream stream = new CopyOnReadInputStream(
                delegate.readBlob(blobName, lastOffset, DEFAULT_BYTE_ARRAY_SIZE),
                ActionListener.wrap(
                    release -> blobCacheService.put(repository, blobName, path(), release, lastOffset),
                    e -> {}
                )
            );
            if (lastOffset < position) {
                stream.skip(position - lastOffset);
            }
            return stream;
        }
        return null;
    }

    /**
     * A {@link FilterInputStream} that copies over all the bytes read from the original input stream to a given {@link ByteArray}. The
     * number of bytes copied cannot exceed the size of the {@link ByteArray}.
     */
    static class CopyOnReadInputStream extends FilterInputStream {

        private final ActionListener<ReleasableBytesReference> listener;
        private final AtomicBoolean closed;
        private final ByteArray bytes;

        private IOException failure;
        private long count;
        private long mark;

        protected CopyOnReadInputStream(InputStream in, ActionListener<ReleasableBytesReference> listener) {
            super(in);
            this.listener = Objects.requireNonNull(listener);
            this.bytes = bigArrays.newByteArray(DEFAULT_BYTE_ARRAY_SIZE);
            this.closed = new AtomicBoolean(false);
        }

        private <T> T handleFailure(CheckedSupplier<T, IOException> supplier) throws IOException {
            try {
                return supplier.get();
            } catch (IOException e) {
                assert failure == null;
                failure = e;
                throw e;
            }
        }

        public int read() throws IOException {
            final int result = handleFailure(super::read);
            if (result != -1) {
                if (count < bytes.size()) {
                    bytes.set(count, (byte) result);
                }
                count++;
            }
            return result;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            final int result = handleFailure(() -> super.read(b, off, len));
            if (result != -1) {
                if (count < bytes.size()) {
                    bytes.set(count, b, off, Math.toIntExact(Math.min(bytes.size() - count, result)));
                }
                count += result;
            }
            return result;
        }

        @Override
        public long skip(long n) throws IOException {
            final long skip = handleFailure(() -> super.skip(n));
            if (skip > 0L) {
                count += skip;
            }
            return skip;
        }

        @Override
        public synchronized void mark(int readlimit) {
            super.mark(readlimit);
            mark = count;
        }

        @Override
        public synchronized void reset() throws IOException {
            handleFailure(() -> {
                super.reset();
                return null;
            });
            count = mark;
        }

        @Override
        public final void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                boolean success = false;
                try {
                    if (failure == null) {
                        PagedBytesReference reference = new PagedBytesReference(bytes, Math.toIntExact(Math.min(count, bytes.size())));
                        listener.onResponse(new ReleasableBytesReference(reference, bytes));
                        success = true;
                    } else {
                        listener.onFailure(failure);
                    }
                } finally {
                    if (success == false) {
                        bytes.close();
                    }
                    in.close();
                }
            }
        }
    }
}
