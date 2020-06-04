/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link FilterDirectory} that records every calls to {@link IndexInput#readByte()}
 * or to {@link IndexInput#readBytes(byte[], int, int)}
 */
public class RecordingDirectory extends FilterDirectory {

    @FunctionalInterface
    interface Recorder {
        void recordRead(String fileName, Long fileLength, String readMethod, long readAt, Long readLength, boolean readFromClone);
    }

    private final Recorder recorder;

    RecordingDirectory(Directory in, Recorder recorder) {
        super(in);
        this.recorder = Objects.requireNonNull(recorder);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final IndexInput in = super.openInput(name, context);
        return new RecordingIndexInput(in, name, recorder);
    }

    static class RecordingIndexInput extends IndexInput {

        private final String fileName;
        private final long fileLength;
        private final IndexInput in;
        private final Recorder recorder;
        private final long offset;

        private AtomicBoolean closed;
        private boolean clone;

        RecordingIndexInput(IndexInput in, String fileName, Recorder recorder) {
            this(in, recorder, fileName, in.length(), 0L);
        }

        RecordingIndexInput(IndexInput in, Recorder recorder, String fileName, long fileLength, long offset) {
            super(in.toString());
            this.in = Objects.requireNonNull(in);
            this.recorder = Objects.requireNonNull(recorder);
            this.fileName = fileName;
            this.fileLength = fileLength;
            this.closed = new AtomicBoolean(false);
            this.offset = offset;
        }

        private synchronized void onRecord(String readMethod, long readAt, long readLength) {
            recorder.recordRead(fileName, fileLength, readMethod, readAt, readLength, clone);
        }

        @Override
        public long length() {
            return in.length();
        }

        @Override
        public long getFilePointer() {
            return in.getFilePointer();
        }

        @Override
        public byte readByte() throws IOException {
            final byte result = in.readByte();
            onRecord("readByte", this.offset + in.getFilePointer(), 1L);
            return result;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            in.readBytes(b, offset, len);
            onRecord("readBytes", this.offset + in.getFilePointer(), len);
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                in.close();
            }
        }

        @Override
        public IndexInput clone() {
            final RecordingIndexInput clone = new RecordingIndexInput(in.clone(), recorder, fileName, fileLength, offset);
            clone.closed = new AtomicBoolean(false);
            clone.clone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String desc, long off, long len) throws IOException {
            final RecordingIndexInput slice = new RecordingIndexInput(in.slice(desc, off, len), recorder, fileName, fileLength, offset + off);
            slice.clone = true;
            return slice;
        }
    }
}
