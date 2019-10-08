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
    private boolean closed = false;

    public BlobStoreIndexInput(final String resourceDesc, final FileInfo fileInfo, final BlobContainer container) {
        super(resourceDesc);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.container = Objects.requireNonNull(container);
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
        ensureOpen();

        if (fileInfo.numberOfParts() == 1) {
            assert getFilePointer() + offset <= length();
            byte[] bytes = container.readBlob(fileInfo.partName(0), Math.toIntExact(getFilePointer() + offset), length); // repository must not be compressed
            System.arraycopy(bytes, 0, b, 0, length);
        }
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
            throw new IOException("BlobStoreIndexInput [" + toString() + "] is closed");
        }
    }

    @Override
    public String toString() {
        return "BlobStoreIndexInput{" + super.toString()  + "}";
    }
}
