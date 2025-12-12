/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdFieldsProducer;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntSupplier;

import static org.elasticsearch.index.codec.bloomfilter.BloomFilterHashFunctions.MurmurHash3.hash64;

public class ES93BloomFilterPostingsFormat extends PostingsFormat {
    public static final String FORMAT_NAME = "ES93BloomFilterPostingsFormat";
    public static final String STORED_FIELDS_BLOOM_FILTER_EXTENSION = "sfbf";
    public static final String STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION = "sfbfm";
    private static final int VERSION_START = 0;
    private static final int VERSION_CURRENT = VERSION_START;

    // We use prime numbers with the Kirsch-Mitzenmacher technique to obtain multiple hashes from two hash functions
    private static final int[] PRIMES = new int[] { 2, 5, 11, 17, 23, 29, 41, 47, 53, 59, 71 };
    private static final int DEFAULT_NUM_HASH_FUNCTIONS = 7;
    private static final byte BLOOM_FILTER_STORED = 1;
    private static final byte BLOOM_FILTER_NOT_STORED = 0;
    private static final ByteSizeValue MAX_BLOOM_FILTER_SIZE = ByteSizeValue.ofMb(8);
    public static final ByteSizeValue DEFAULT_BLOOM_FILTER_SIZE = ByteSizeValue.ofKb(256);

    private final BigArrays bigArrays;
    private final int numHashFunctions;
    private final int bloomFilterSizeInBits;

    public ES93BloomFilterPostingsFormat() {
        super(FORMAT_NAME);
        this.bigArrays = null;
        this.numHashFunctions = 0;
        this.bloomFilterSizeInBits = 0;
    }

    public ES93BloomFilterPostingsFormat(BigArrays bigArrays) {
        super(FORMAT_NAME);
        this.bigArrays = bigArrays;
        this.numHashFunctions = DEFAULT_NUM_HASH_FUNCTIONS;
        this.bloomFilterSizeInBits = Math.multiplyExact((int) DEFAULT_BLOOM_FILTER_SIZE.getBytes(), Byte.SIZE);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new Writer(state, bigArrays, DEFAULT_NUM_HASH_FUNCTIONS, () -> bloomFilterSizeInBits, IdFieldMapper.NAME);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new Reader(state);
    }

    static class Writer extends FieldsConsumer {
        private final SegmentWriteState state;
        private final IntSupplier defaultBloomFilterSizeInBitsSupplier;
        private final int numHashFunctions;
        private final String bloomFilterFieldName;
        private final List<Closeable> toClose = new ArrayList<>();

        private final IndexOutput metadataOut;
        private final IndexOutput bloomFilterDataOut;
        private final int bitsetSizeInBits;
        private final int bitSetSizeInBytes;
        private final ByteArray buffer;
        private final int[] hashes;
        private boolean closed;

        Writer(
            SegmentWriteState state,
            BigArrays bigArrays,
            int numHashFunctions,
            IntSupplier defaultBloomFilterSizeInBitsSupplier,
            String bloomFilterFieldName
        ) throws IOException {
            final SegmentInfo segmentInfo = state.segmentInfo;
            final IOContext context = state.context;
            this.state = state;
            this.defaultBloomFilterSizeInBitsSupplier = defaultBloomFilterSizeInBitsSupplier;
            assert numHashFunctions <= PRIMES.length
                : "Number of hash functions must be <= " + PRIMES.length + " but was " + numHashFunctions;

            this.numHashFunctions = numHashFunctions;
            this.hashes = new int[numHashFunctions];
            this.bloomFilterFieldName = bloomFilterFieldName;

            boolean success = false;
            try {
                metadataOut = state.directory.createOutput(bloomFilterMetadataFileName(segmentInfo, state.segmentSuffix), context);
                toClose.add(metadataOut);
                CodecUtil.writeIndexHeader(metadataOut, FORMAT_NAME, VERSION_CURRENT, segmentInfo.getId(), state.segmentSuffix);

                bloomFilterDataOut = state.directory.createOutput(bloomFilterFileName(segmentInfo, state.segmentSuffix), context);
                toClose.add(bloomFilterDataOut);

                CodecUtil.writeIndexHeader(bloomFilterDataOut, FORMAT_NAME, VERSION_CURRENT, segmentInfo.getId(), state.segmentSuffix);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }

            this.bitsetSizeInBits = defaultBloomFilterSizeInBitsSupplier.getAsInt();
            this.bitSetSizeInBytes = bitsetSizeInBits / Byte.SIZE;
            this.buffer = bigArrays.newByteArray(bitSetSizeInBytes);
            toClose.add(buffer);
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            final Terms terms = fields.terms(bloomFilterFieldName);
            if (terms == null) {
                throw new IllegalStateException("terms field [" + bloomFilterFieldName + "] not found");
            }
            assert state.fieldInfos.fieldInfo(bloomFilterFieldName) != null;

            var termsEnum = terms.iterator();
            while (true) {
                final BytesRef term = termsEnum.next();
                if (term == null) {
                    break;
                }
                add(term);
            }
        }

        private void add(BytesRef value) {
            var termHashes = hashTerm(value, hashes);
            for (int hash : termHashes) {
                final int posInBitArray = hash & (bitsetSizeInBits - 1);
                final int pos = posInBitArray >> 3; // div 8
                final int mask = 1 << (posInBitArray & 7); // mod 8
                final byte val = (byte) (buffer.get(pos) | mask);
                buffer.set(pos, val);
            }
        }

        private int getBloomFilterSizeInBits() {
            int bloomFilterSizeInBits = defaultBloomFilterSizeInBitsSupplier.getAsInt();
            assert isPowerOfTwo(bloomFilterSizeInBits) : "Bloom filter size is not a power of 2: " + bloomFilterSizeInBits;
            return bloomFilterSizeInBits;
        }

        private void flush() throws IOException {
            BloomFilterMetadata bloomFilterMetadata = new BloomFilterMetadata(
                bloomFilterDataOut.getFilePointer(),
                bitsetSizeInBits,
                numHashFunctions
            );

            if (buffer.hasArray()) {
                bloomFilterDataOut.writeBytes(buffer.array(), 0, bitSetSizeInBytes);
            } else {
                BytesReference.fromByteArray(buffer, bitSetSizeInBytes).writeTo(new IndexOutputOutputStream(bloomFilterDataOut));
            }

            CodecUtil.writeFooter(bloomFilterDataOut);

            // TODO: this is not necessary
            if (bloomFilterMetadata != null) {
                metadataOut.writeByte(BLOOM_FILTER_STORED);

            } else {
                metadataOut.writeByte(BLOOM_FILTER_NOT_STORED);
            }
            bloomFilterMetadata.writeTo(metadataOut);
            CodecUtil.writeFooter(metadataOut);
        }

        @Override
        public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
            if (useOptimizedMerge(mergeState)) {
                mergeOptimized(mergeState);
            } else {
                super.merge(mergeState, norms);
            }
        }

        private void mergeOptimized(MergeState mergeState) throws IOException {
            assert useOptimizedMerge(mergeState);

            if (mergeState.fieldsProducers.length == 0) {
                return;
            }
            // assert mergeState.fieldsProducers[0] instanceof Reader;
            // Reader firstReader = (Reader) mergeState.fieldsProducers[0];
            // assert firstReader.bloomFilterFieldReader != null;

            mergeBloomFiltersWithOr(mergeState);
        }

        /**
         * Determines whether bloom filters can be merged using a bitwise OR operation.
         *
         * <p>Fast merging is possible when all segments in the merge state satisfy two conditions:
         * <ul>
         *   <li>Each segment has an associated bloom filter
         *   <li>All bloom filters have identical dimensions (same bit array size)
         * </ul>
         *
         * <p>When these conditions are met, the bloom filters can be efficiently combined by
         * performing a bitwise OR across their underlying bitsets, avoiding the need to
         * re-hash and re-insert elements.
         *
         * @param mergeState the merge state containing segments to be merged
         * @return {@code true} if all segments have compatible bloom filters that can be
         *         merged via bitwise OR; {@code false} otherwise
         */
        private boolean useOptimizedMerge(MergeState mergeState) throws IOException {
            int expectedBloomFilterSize = -1;
            for (int readerIndex = 0; readerIndex < mergeState.fieldsProducers.length; readerIndex++) {
                final FieldsProducer f = mergeState.fieldsProducers[readerIndex];
                var terms = f.terms(bloomFilterFieldName);
                if (terms instanceof DelegatingBloomFilterFieldsProducer.BloomFilterTerms == false) {
                    return false;
                }

                DelegatingBloomFilterFieldsProducer.BloomFilterTerms reader = (DelegatingBloomFilterFieldsProducer.BloomFilterTerms) terms;
                BloomFilterFieldReader bloomFilterFieldReader = reader.getBloomFilter();

                if (bloomFilterFieldReader == null) {
                    return false;
                }

                if (expectedBloomFilterSize == -1) {
                    expectedBloomFilterSize = bloomFilterFieldReader.bloomFilterBitSetSizeInBits;
                }

                if (bloomFilterFieldReader.bloomFilterBitSetSizeInBits != expectedBloomFilterSize) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }

            flush();
            IOUtils.close(toClose);
            closed = true;
        }

        private void mergeBloomFiltersWithOr(MergeState mergeState) throws IOException {
            for (int readerIdx = 0; readerIdx < mergeState.fieldsProducers.length; readerIdx++) {
                FieldsProducer fieldsProducer = mergeState.fieldsProducers[readerIdx];
                var terms = fieldsProducer.terms(bloomFilterFieldName);
                if (terms instanceof DelegatingBloomFilterFieldsProducer.BloomFilterTerms == false) {
                    throw new IllegalStateException("Expected a Reader but got " + fieldsProducer.getClass());
                }

                DelegatingBloomFilterFieldsProducer.BloomFilterTerms reader = (DelegatingBloomFilterFieldsProducer.BloomFilterTerms) terms;
                BloomFilterFieldReader bloomFilterFieldReader = reader.getBloomFilter();

                if (bloomFilterFieldReader != null) {
                    assert bloomFilterFieldReader.getBloomFilterBitSetSizeInBits() == bitsetSizeInBits
                        : "Expected a bloom filter bitset size "
                            + bitsetSizeInBits
                            + " but got "
                            + bloomFilterFieldReader.getBloomFilterBitSetSizeInBits();
                    bloomFilterFieldReader.checkIntegrity();
                    RandomAccessInput bloomFilterData = bloomFilterFieldReader.bloomFilterIn;

                    bloomFilterData.prefetch(0, bitSetSizeInBytes);
                    for (int i = 0; i < bitSetSizeInBytes; i++) {
                        var existingBloomFilterByte = bloomFilterData.readByte(i);
                        var resultingBloomFilterByte = buffer.get(i);
                        buffer.set(i, (byte) (existingBloomFilterByte | resultingBloomFilterByte));
                    }
                }
            }
        }

    }

    static class Reader extends FieldsProducer {
        private final BloomFilterFieldReader bloomFilterFieldReader;
        private final FieldsProducer delegate;

        Reader(SegmentReadState state) throws IOException {
            DocValuesProducer docValuesProducer = null;
            boolean success = false;
            try {
                var codec = state.segmentInfo.getCodec();
                this.bloomFilterFieldReader = BloomFilterFieldReader.open(state);

                // Erase the segment suffix (used only for reading postings)
                docValuesProducer = codec.docValuesFormat().fieldsProducer(new SegmentReadState(state, ""));
                var fieldsProducer = new TSDBSyntheticIdFieldsProducer(state, docValuesProducer);
                success = true;
                this.delegate = new DelegatingBloomFilterFieldsProducer(fieldsProducer, bloomFilterFieldReader);
            } finally {
                if (success == false) {
                    IOUtils.close(docValuesProducer);
                }
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            bloomFilterFieldReader.checkIntegrity();
            delegate.checkIntegrity();
        }

        @Override
        public Iterator<String> iterator() {
            return delegate.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            return delegate.terms(field);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(bloomFilterFieldReader, delegate);
        }
    }

    static class BloomFilterFieldReader implements BloomFilter {
        private final IndexInput bloomFilterData;
        private final RandomAccessInput bloomFilterIn;
        private final int bloomFilterBitSetSizeInBits;
        private final int[] hashes;

        @Nullable
        static BloomFilterFieldReader open(SegmentReadState state) throws IOException {
            final Directory directory = state.directory;
            final SegmentInfo si = state.segmentInfo;
            final String segmentSuffix = state.segmentSuffix;
            final FieldInfos fn = state.fieldInfos;
            final IOContext context = state.context;
            List<Closeable> toClose = new ArrayList<>();
            boolean success = false;
            try (var metaInput = directory.openChecksumInput(bloomFilterMetadataFileName(si, segmentSuffix))) {
                var metadataVersion = CodecUtil.checkIndexHeader(
                    metaInput,
                    FORMAT_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    si.getId(),
                    segmentSuffix
                );
                var hasBloomFilter = metaInput.readByte() == BLOOM_FILTER_STORED;
                if (hasBloomFilter == false) {
                    return null;
                }
                BloomFilterMetadata bloomFilterMetadata = BloomFilterMetadata.readFrom(metaInput, fn);
                CodecUtil.checkFooter(metaInput);

                IndexInput bloomFilterData = directory.openInput(bloomFilterFileName(si, segmentSuffix), context);
                toClose.add(bloomFilterData);
                var bloomFilterDataVersion = CodecUtil.checkIndexHeader(
                    bloomFilterData,
                    FORMAT_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    si.getId(),
                    segmentSuffix
                );

                if (metadataVersion != bloomFilterDataVersion) {
                    throw new CorruptIndexException(
                        "Format versions mismatch: meta=" + metadataVersion + ", data=" + bloomFilterDataVersion,
                        bloomFilterData
                    );
                }
                CodecUtil.retrieveChecksum(bloomFilterData);

                var bloomFilterFieldReader = new BloomFilterFieldReader(
                    bloomFilterData.randomAccessSlice(bloomFilterMetadata.fileOffset(), bloomFilterMetadata.sizeInBytes()),
                    bloomFilterMetadata.sizeInBits(),
                    bloomFilterMetadata.numHashFunctions(),
                    bloomFilterData
                );
                success = true;
                return bloomFilterFieldReader;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }

        BloomFilterFieldReader(
            RandomAccessInput bloomFilterIn,
            int bloomFilterBitSetSizeInBits,
            int numHashFunctions,
            IndexInput bloomFilterData
        ) {
            this.bloomFilterIn = bloomFilterIn;
            this.bloomFilterBitSetSizeInBits = bloomFilterBitSetSizeInBits;
            this.hashes = new int[numHashFunctions];
            this.bloomFilterData = bloomFilterData;
        }

        public boolean mayContainTerm(String field, BytesRef term) throws IOException {
            // assert fieldInfo.getName().equals(field);

            var termHashes = hashTerm(term, hashes);

            for (int hash : termHashes) {
                final int posInBitArray = hash & (bloomFilterBitSetSizeInBits - 1);
                final int pos = posInBitArray >> 3; // div 8
                final int mask = 1 << (posInBitArray & 7); // mod 8
                final byte bits = bloomFilterIn.readByte(pos);
                if ((bits & mask) == 0) {
                    return false;
                }
            }
            return true;
        }

        int getBloomFilterBitSetSizeInBits() {
            return bloomFilterBitSetSizeInBits;
        }

        void checkIntegrity() throws IOException {
            CodecUtil.checksumEntireFile(bloomFilterData);
        }

        @Override
        public void close() throws IOException {
            bloomFilterData.close();
        }

        @Override
        public String toString() {
            return bloomFilterData.toString();
        }
    }

    record BloomFilterMetadata(long fileOffset, int sizeInBits, int numHashFunctions) {
        BloomFilterMetadata {
            assert isPowerOfTwo(sizeInBits);
        }

        int sizeInBytes() {
            return sizeInBits / Byte.SIZE;
        }

        void writeTo(IndexOutput indexOut) throws IOException {
            indexOut.writeVLong(fileOffset);
            indexOut.writeVInt(sizeInBits);
            indexOut.writeVInt(numHashFunctions);
        }

        static BloomFilterMetadata readFrom(IndexInput in, FieldInfos fieldInfos) throws IOException {
            final long fileOffset = in.readVLong();
            final int bloomFilterSizeInBits = in.readVInt();
            final int numOfHashFunctions = in.readVInt();
            return new BloomFilterMetadata(fileOffset, bloomFilterSizeInBits, numOfHashFunctions);
        }
    }

    private static int[] hashTerm(BytesRef value, int[] outputs) {
        long hash64 = hash64(value.bytes, value.offset, value.length);
        // First use output splitting to get two hash values out of a single hash function
        int upperHalf = (int) (hash64 >> Integer.SIZE);
        int lowerHalf = (int) hash64;
        // Then use the Kirsch-Mitzenmacher technique to obtain multiple hashes efficiently
        for (int i = 0; i < outputs.length; i++) {
            // Use prime numbers as the constant for the KM technique so these don't have a common gcd
            outputs[i] = (lowerHalf + PRIMES[i] * upperHalf) & 0x7FFF_FFFF; // Clears sign bit, gives positive 31-bit values
        }
        return outputs;
    }

    private static boolean isPowerOfTwo(int value) {
        return (value & (value - 1)) == 0;
    }

    private static String bloomFilterMetadataFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION);
    }

    private static String bloomFilterFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_BLOOM_FILTER_EXTENSION);
    }
}
