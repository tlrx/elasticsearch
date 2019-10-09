package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.BufferedIndexInput;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

public class BlobStoreIndexInput extends BufferedIndexInput {

    private final FileInfo fileInfo;
    private final BlobContainer container;
    private volatile boolean closed = false;

    // we might want to tweak the BufferedIndexInput's buffer too
    // (could be another index setting)
    BlobStoreIndexInput(final String resourceDesc, final FileInfo fileInfo, final BlobContainer container) {
        super(resourceDesc);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.container = Objects.requireNonNull(container);
    }

    @Override
    protected void readInternal(byte[] buffer, int offset, int length) throws IOException {
        ensureOpen();

        final long partSize = fileInfo.partSize().getBytes();
        final long adjustedOffset = getFilePointer() + offset;
        assert adjustedOffset < length();

        int readBytes = 0;
        for (long part = adjustedOffset / partSize; part < fileInfo.numberOfParts() && readBytes < length; part++) {
            // adjusted offset in this part to start to read from
            long offsetPart = adjustedOffset - (part * partSize);
            // how many bytes to read in this part
            int lengthPart = Math.toIntExact(Math.min((long) length, fileInfo.partBytes((int) part)));

            // not sure how it works with compressed files
            byte[] bytes = container.readBlob(fileInfo.partName(part), Math.toIntExact(offsetPart), lengthPart);
            assert bytes.length == lengthPart;

            System.arraycopy(bytes, 0, buffer, readBytes, lengthPart);
            readBytes += lengthPart;
        }
        assert readBytes == length;

/*
        if (fileInfo.numberOfParts() == 1) {
            assert getFilePointer() + offset <= length();
            byte[] bytes = container.readBlob(fileInfo.partName(0), Math.toIntExact(getFilePointer() + offset), length); // repository must not be compressed
            System.arraycopy(bytes, 0, b, 0, length);
        }

 */
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        ensureOpen();
        if (pos > length()) {
            throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
        }
    }

    @Override
    public long length() {
        return fileInfo.length();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException(toString() + " is closed");
        }
    }

    @Override
    public String toString() {
        return "BlobStoreIndexInput(" + super.toString()  + ")";
    }
}
