package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;

public class BlobStoreIndexInput extends IndexInput {

    private final BlobContainer container;
    private final FileInfo fileInfo;
    private final long fileLength;

    private InputStream currentStream;
    private long initialPosition;
    private long currentPosition;
    private boolean initialized;
    private boolean closed;

    BlobStoreIndexInput(BlobContainer container, FileInfo fileInfo) {
        this(fileInfo.name(), container, fileInfo, fileInfo.length(), 0L);
    }

    private BlobStoreIndexInput(String name, BlobContainer container, FileInfo fileInfo, long fileLength, long initialPosition) {
        super(resourceDesc(name, container, fileInfo, fileLength));
        this.container = Objects.requireNonNull(container);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.fileLength = fileLength;
        this.initialPosition = initialPosition;
        this.currentPosition = this.initialPosition;
    }

    @Override
    public long length() {
        return fileLength;
    }

    @Override
    public long getFilePointer() {
        return currentPosition - initialPosition;
    }

    @Nullable
    private InputStream currentStream() throws IOException {
        ensureOpen();
        if (currentStream == null) {
            return initialized ? null : nextStream();
        }
        return currentStream;
    }

    @Nullable
    private InputStream nextStream() throws IOException {
        return openStream(currentPosition);
    }

    @Nullable
    private InputStream openStream(final long position) throws IOException {
        assert initialized == false || currentStream != null || position == fileLength;
        ensureOpen();
        closeStream(currentStream);
        try {
            final Optional<Tuple<Long, Long>> part = findPart(position);
            if (part.isEmpty()) {
                currentStream = null;
                return currentStream;
            }
            final long partNumber = part.get().v1();
            final long partPosition = part.get().v2();
            final long partSize = fileInfo.partBytes((int) partNumber);
            if (partPosition < partSize) {
                currentStream = container.readBlob(fileInfo.partName(partNumber), partPosition, partSize - partPosition);
            } else {
                currentStream = null;
            }
            currentPosition = position;
            return currentStream;
        } finally {
            initialized = true;
        }
    }

    private long closeStream(final InputStream inputStream) {
        long byteCount = 0;
        if (inputStream != null) {
            try {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    byteCount += bytesRead;
                }
            } catch (IOException e) {

            } finally {
                IOUtils.closeWhileHandlingException(inputStream);
            }
        }
        return byteCount;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("Reading past end of file [pos=" + pos + ", length=" + length() + "] for " + toString());
        } else if (pos < 0L) {
            throw new IOException("Seeking to negative position [" + pos + "] for " + toString());
        }
        final long seekingPosition = pos + initialPosition;
        if (seekingPosition == currentPosition) {
            return;
        }
        if (seekingPosition < currentPosition || (seekingPosition - currentPosition > ByteSizeUnit.MB.toBytes(1L))) {
            openStream(seekingPosition);
        } else {
            skipBytes(seekingPosition - currentPosition);
            assert currentPosition == seekingPosition;
        }
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        if (offset >= 0L && length >= 0L && offset + length <= length()) {
            return new BlobStoreIndexInput(sliceDescription, container, fileInfo, length, initialPosition + offset);
        } else {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset
                + ",length=" + length + ",fileLength=" + length() + ": " + this);
        }
    }

    @Override
    public IndexInput clone() {
        return new BlobStoreIndexInput(toString(), container, fileInfo, fileLength, initialPosition);
    }

    @Override
    public byte readByte() throws IOException {
        ensureOpen();
        final InputStream stream = currentStream();
        if (stream == null) {
            return -1;
        }
        final int read = stream.read();
        if (read == -1) {
            nextStream();
            return readByte();
        }
        currentPosition += 1;
        return (byte) read;
    }

    @Override
    public void readBytes(byte[] buffer, int offset, int length) throws IOException {
        ensureOpen();
        final InputStream stream = currentStream();
        if (stream == null) {
            return;
        }
        final int read = stream.read(buffer, offset, length);
        if (read == -1) {
            nextStream();
            readBytes(buffer, offset, length);
        } else if (read > 0) {
            currentPosition += read;
            int remaining = length - read;
            if (remaining > 0) {
                readBytes(buffer, offset + read, remaining);
            }
        }
    }

    @Override
    public void close() throws IOException {
        closeStream(currentStream);
        closed = true;
        currentStream = null;
        initialized = true;
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "cannot use BlobStoreIndexInput after close";
            throw new IllegalStateException("BlobStoreIndexInput is closed");
        }
    }

    private static String resourceDesc(String name, BlobContainer container, FileInfo fileInfo, long length) {
        return "BlobStoreIndexInput(name=" + name
            + ", path=" + (container != null ? container.path().toString() : "null")
            + ", length=" + length
            + ", fileInfo=" + fileInfo + ")";
    }

    private Optional<Tuple<Long, Long>> findPart(final long position) {
        final long partNumber = position / fileInfo.partSize().getBytes();
        if (partNumber > fileInfo.numberOfParts()) {
            return Optional.empty();
        }
        final long partPosition = position % fileInfo.partSize().getBytes();
        if (partPosition == 0L && partNumber == fileInfo.numberOfParts()) {
            return Optional.of(Tuple.tuple(partNumber - 1L, fileInfo.partBytes((int) (partNumber - 1))));
        }
        return Optional.of(Tuple.tuple(partNumber, partPosition));
    }
}
