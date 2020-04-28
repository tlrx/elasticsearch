/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

class ChecksumFooterIndexInput extends IndexInput {

    private final byte[] footer;
    private final long length;
    private final long offset;

    private int position;

    private ChecksumFooterIndexInput(String name, long length, byte[] footer) {
        super("ChecksumFooterIndexInput(" + name + ')');
        assert footer.length == CodecUtil.footerLength();
        this.footer = Objects.requireNonNull(footer);
        this.length = length;
        this.offset = length - this.footer.length;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long getFilePointer() {
        return offset + position;
    }

    @Override
    public byte readByte() throws IOException {
        if (getFilePointer() >= length()) {
            throw new EOFException("seek past EOF");
        }
        return footer[position++];
    }

    @Override
    public void readBytes(final byte[] b, final int off, int len) throws IOException {
        if (getFilePointer() + len > length()) {
            throw new EOFException("seek past EOF");
        }
        System.arraycopy(footer, position, b, off, len);
        position += len;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException("Seeking to negative position: " + pos);
        } else if (pos > length()) {
            throw new EOFException("seek past EOF");
        } else if (pos < offset) {
            throw new EOFException("Can't read before footer");
        }
        position = Math.toIntExact(pos - offset);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }

    public static ChecksumFooterIndexInput create(final String name, final long length, final String checksum) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (IndexOutput output = new ByteBuffersIndexOutput(out, "create ChecksumFooterIndexInput", name)) {
            // See CodecUtil.writeFooter()
            output.writeInt(CodecUtil.FOOTER_MAGIC);
            output.writeInt(0);
            output.writeLong(Long.parseLong(checksum, Character.MAX_RADIX));
            output.close();
            return new ChecksumFooterIndexInput(name, length, out.toArrayCopy());
        }
    }
}
