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
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
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
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

import static org.elasticsearch.index.codec.bloomfilter.BloomFilterHashFunctions.MurmurHash3.hash64;

public class ES93BloomFilterDocValuesFormat extends DocValuesFormat {
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
    public static final ByteSizeValue DEFAULT_BLOOM_FILTER_SIZE = ByteSizeValue.ofMb(1);

    private final BigArrays bigArrays;
    private final int numHashFunctions;
    private final int bloomFilterSizeInBits;

    public ES93BloomFilterDocValuesFormat() {
        super(FORMAT_NAME);
        bigArrays = null;
        numHashFunctions = 0;
        bloomFilterSizeInBits = 0;
    }

    public ES93BloomFilterDocValuesFormat(BigArrays bigArrays) {
        super(FORMAT_NAME);
        this.bigArrays = bigArrays;
        this.numHashFunctions = DEFAULT_NUM_HASH_FUNCTIONS;
        this.bloomFilterSizeInBits = Math.multiplyExact((int) DEFAULT_BLOOM_FILTER_SIZE.getBytes(), Byte.SIZE);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new Writer(state, bigArrays, numHashFunctions, () -> bloomFilterSizeInBits, IdFieldMapper.NAME);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new Reader(state);
    }

    static class Writer extends DocValuesConsumer {
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
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            var values = valuesProducer.getBinary(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                BytesRef term = values.binaryValue();
                var termHashes = hashTerm(term, hashes);
                for (int hash : termHashes) {
                    final int posInBitArray = hash & (bitsetSizeInBits - 1);
                    final int pos = posInBitArray >> 3; // div 8
                    final int mask = 1 << (posInBitArray & 7); // mod 8
                    final byte val = (byte) (buffer.get(pos) | mask);
                    buffer.set(pos, val);
                }
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
        public void merge(MergeState mergeState) throws IOException {
            if (useOptimizedMerge(mergeState)) {
                mergeOptimized(mergeState);
            } else {
                super.merge(mergeState);
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
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                final FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(bloomFilterFieldName);
                if (fieldInfo == null) {
                    continue;
                }
                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                if (docValuesProducer instanceof FilterDocValuesProducer filterDocValuesProducer) {
                    docValuesProducer = filterDocValuesProducer.getIn();
                }
                var binaryDocValues = docValuesProducer.getBinary(fieldInfo);

                if (binaryDocValues instanceof Reader.BloomFilterDocValues == false) {
                    return false;
                }

                var bloomFilterDocValues = (Reader.BloomFilterDocValues) binaryDocValues;

                BloomFilterFieldReader bloomFilterFieldReader = bloomFilterDocValues.bloomFilterFieldReader();

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
            for (int readerIdx = 0; readerIdx < mergeState.docValuesProducers.length; readerIdx++) {
                final FieldInfo fieldInfo = mergeState.fieldInfos[readerIdx].fieldInfo(bloomFilterFieldName);
                if (fieldInfo == null) {
                    continue;
                }
                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[readerIdx];
                if (docValuesProducer instanceof FilterDocValuesProducer filterDocValuesProducer) {
                    docValuesProducer = filterDocValuesProducer.getIn();
                }

                var binaryDocValues = docValuesProducer.getBinary(fieldInfo);

                if (binaryDocValues instanceof Reader.BloomFilterDocValues == false) {
                    throw new IllegalStateException("Expected a Reader but got " + binaryDocValues);
                }

                var bloomFilterDocValues = (Reader.BloomFilterDocValues) binaryDocValues;

                BloomFilterFieldReader bloomFilterFieldReader = bloomFilterDocValues.bloomFilterFieldReader();
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

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {

        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {

        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {

        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {

        }
    }

    static class Reader extends DocValuesProducer implements BloomFilter {
        private final BloomFilterFieldReader bloomFilterFieldReader;

        Reader(SegmentReadState state) throws IOException {
            bloomFilterFieldReader = BloomFilterFieldReader.open(state);
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            return new NumericDocValues() {
                @Override
                public long longValue() throws IOException {
                    return 0;
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return false;
                }

                @Override
                public int docID() {
                    return NO_MORE_DOCS;
                }

                @Override
                public int nextDoc() throws IOException {
                    return NO_MORE_DOCS;
                }

                @Override
                public int advance(int target) throws IOException {
                    return NO_MORE_DOCS;
                }

                @Override
                public long cost() {
                    return 0;
                }
            };
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            return new BloomFilterDocValues();
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            return new SortedDocValues() {
                @Override
                public int ordValue() throws IOException {
                    return 0;
                }

                @Override
                public BytesRef lookupOrd(int ord) throws IOException {
                    return null;
                }

                @Override
                public int getValueCount() {
                    return 0;
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return false;
                }

                @Override
                public int docID() {
                    return 0;
                }

                @Override
                public int nextDoc() throws IOException {
                    return 0;
                }

                @Override
                public int advance(int target) throws IOException {
                    return 0;
                }

                @Override
                public long cost() {
                    return 0;
                }
            };
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return new SortedNumericDocValues() {
                @Override
                public long nextValue() throws IOException {
                    return 0;
                }

                @Override
                public int docValueCount() {
                    return 0;
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return false;
                }

                @Override
                public int docID() {
                    return 0;
                }

                @Override
                public int nextDoc() throws IOException {
                    return 0;
                }

                @Override
                public int advance(int target) throws IOException {
                    return 0;
                }

                @Override
                public long cost() {
                    return 0;
                }
            };
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            return new SortedSetDocValues() {
                @Override
                public long nextOrd() throws IOException {
                    return 0;
                }

                @Override
                public int docValueCount() {
                    return 0;
                }

                @Override
                public BytesRef lookupOrd(long ord) throws IOException {
                    return null;
                }

                @Override
                public long getValueCount() {
                    return 0;
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return false;
                }

                @Override
                public int docID() {
                    return 0;
                }

                @Override
                public int nextDoc() throws IOException {
                    return 0;
                }

                @Override
                public int advance(int target) throws IOException {
                    return 0;
                }

                @Override
                public long cost() {
                    return 0;
                }
            };
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
            return new DocValuesSkipper() {
                @Override
                public void advance(int target) throws IOException {

                }

                @Override
                public int numLevels() {
                    return 0;
                }

                @Override
                public int minDocID(int level) {
                    return 0;
                }

                @Override
                public int maxDocID(int level) {
                    return 0;
                }

                @Override
                public long minValue(int level) {
                    return 0;
                }

                @Override
                public long maxValue(int level) {
                    return 0;
                }

                @Override
                public int docCount(int level) {
                    return 0;
                }

                @Override
                public long minValue() {
                    return 0;
                }

                @Override
                public long maxValue() {
                    return 0;
                }

                @Override
                public int docCount() {
                    return 0;
                }
            };
        }

        @Override
        public void checkIntegrity() throws IOException {
            bloomFilterFieldReader.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            bloomFilterFieldReader.close();
        }

        @Override
        public boolean mayContainTerm(String field, BytesRef term) throws IOException {
            return bloomFilterFieldReader.mayContainTerm(field, term);
        }

        private class BloomFilterDocValues extends BinaryDocValues implements BloomFilter {
            @Override
            public BytesRef binaryValue() throws IOException {
                return null;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return false;
            }

            @Override
            public int docID() {
                return NO_MORE_DOCS;
            }

            @Override
            public int nextDoc() throws IOException {
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) throws IOException {
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 0;
            }

            @Override
            public boolean mayContainTerm(String field, BytesRef term) throws IOException {
                return bloomFilterFieldReader.mayContainTerm(field, term);
            }

            @Override
            public void close() throws IOException {
                bloomFilterFieldReader.close();
            }

            BloomFilterFieldReader bloomFilterFieldReader() {
                return bloomFilterFieldReader;
            }

            @Override public String toString() {
                return bloomFilterFieldReader.toString();
            }
        }
    }

    static class BloomFilterFieldReader implements BloomFilter {
        private final IndexInput bloomFilterData;
        private final String segmentName;
        private final int maxDoc;
        private final RandomAccessInput bloomFilterIn;
        private final int bloomFilterBitSetSizeInBits;
        private final int[] hashes;

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
                    bloomFilterData,
                    si.name,
                    si.maxDoc()
                );
                success = true;
                return bloomFilterFieldReader;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }
        private final LongAdder numDocs = new LongAdder();
        private final LongAdder falsePositives = new LongAdder();
        private final Logger logger = LogManager.getLogger(ES93BloomFilterDocValuesFormat.class);


        BloomFilterFieldReader(
            RandomAccessInput bloomFilterIn,
            int bloomFilterBitSetSizeInBits,
            int numHashFunctions,
            IndexInput bloomFilterData,
            String segmentName, int maxDoc) {
            this.bloomFilterIn = bloomFilterIn;
            this.bloomFilterBitSetSizeInBits = bloomFilterBitSetSizeInBits;
            this.hashes = new int[numHashFunctions];
            this.bloomFilterData = bloomFilterData;
            this.segmentName = segmentName;
            this.maxDoc = maxDoc;
        }

        public boolean mayContainTerm(String field, BytesRef term) throws IOException {
            // assert fieldInfo.getName().equals(field);
            numDocs.increment();
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
            falsePositives.increment();
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
            var total = numDocs.sum();
            var falseP = falsePositives.sum();
            logger.info("--> total checks: {}, false positives: {}, false positive ratio {} {} - {}", total, falseP, (double) falseP / total, segmentName, maxDoc);
            bloomFilterData.close();
        }

        @Override
        public String toString() {
            return "BloomFilterFieldReader{"
                + "hashes="
                + hashes.length
                + ", bloomFilterBitSetSizeInBits="
                + bloomFilterBitSetSizeInBits
                + ", bloomFilterData="
                + bloomFilterData
                + '}';
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
