package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.BufferedIndexInput;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

public class BlobStoreIndexInput extends BufferedIndexInput {

    private static final Logger logger = LogManager.getLogger(BlobStoreIndexInput.class);

    private final FileInfo fileInfo;
    private final BlobContainer container;
    private volatile boolean closed = false;

    BlobStoreIndexInput(final String resourceDesc, final FileInfo fileInfo, final BlobContainer container, final int buffer) {
        super(resourceDesc, buffer);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.container = Objects.requireNonNull(container);
    }

    @Override
    protected void readInternal(byte[] buffer, int offset, int length) throws IOException {
        logger.trace("readInternal ({}, {}) filepointer {} buffer.length {} offset {} length {}",
            fileInfo.name(), fileInfo.length(),  getFilePointer(), buffer.length, offset, length);
        assert getFilePointer() + length <= length();
        ensureOpen();

        final long partSize = fileInfo.partSize().getBytes();
        int readBytes = 0;
        for (long part = getFilePointer() / partSize; part < fileInfo.numberOfParts() && readBytes < length; part++) {
            // adjusted offset in this part to start to read from
            long offsetPart = getFilePointer() - (part * partSize + readBytes);
            // how many bytes to read in this part
            int lengthPart = Math.toIntExact(Math.min((long) (length - readBytes), fileInfo.partBytes((int) part) - offsetPart));

            logger.trace("read_internal {} part {} offsetPart {} lengthPart {} length {} readBytes {}, partSize {}",
                fileInfo.name(), part, offsetPart, lengthPart, length, readBytes, fileInfo.partBytes((int) part));

            // not sure how it works with compressed files
            byte[] bytes = new byte[0];
            try {
                bytes = container.readBlob(fileInfo.partName(part), Math.toIntExact(offsetPart), lengthPart);
                assert bytes.length == lengthPart;
                System.arraycopy(bytes, 0, buffer, readBytes, lengthPart);
                readBytes += lengthPart;
            } catch (Exception e) {
                long finalPart = part;
                byte[] finalBytes = bytes;
                int finalReadBytes = readBytes;
                logger.error(() ->
                        new ParameterizedMessage("exception in {} offsetPart {} lengthPart {} part {} bytes.length {} " +
                            "buffer.length {}, readBytes, lengthPart", fileInfo.name(), offsetPart, lengthPart, finalPart,
                            finalBytes.length, buffer.length, finalReadBytes, lengthPart), e);
            }

        }
        assert readBytes == length;
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
        logger.trace("close_internal {} size {}", fileInfo.name(), fileInfo.length());
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
