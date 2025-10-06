/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Objects;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class SyntheticIdTermsEnum extends BaseTermsEnum {

    private final SortedDocValues tsIds;// sorted asc. order
    private final SortedNumericDocValues timestamps;  // sorted desc. order

    private SyntheticDocIdSetIterator iterator;

    public SyntheticIdTermsEnum(SortedDocValues  tsIdsDocValues, SortedNumericDocValues timestampsDocValues) throws IOException {
        this.tsIds = Objects.requireNonNull(tsIdsDocValues);
        this.timestamps = Objects.requireNonNull(timestampsDocValues);
        this.iterator = createEmptyIterator();
    }

    /**
     * This is an optional method as per the {@link TermsEnum#ord()} documentation that is not supported by the current implementation.
     * This method always throws an {@link UnsupportedOperationException}.
     */
    @Override
    public long ord() throws IOException {
        throw unsupportedException();
    }

    /**
     * This is an optional method as per the {@link TermsEnum#ord()} documentation that is not supported by the current implementation.
     * This method always throws an {@link UnsupportedOperationException}.
     */
    @Override
    public void seekExact(long ord) throws IOException {
        throw unsupportedException();
    }

    private void ensurePositioned() {
        if (iterator.tsIdOrdinal() == -1
            || iterator.tsIdOrdinal() >= iterator.tsIdOrdinalMax()
            || iterator.docID() == DocIdSetIterator.NO_MORE_DOCS) {
            assert false;
            throw new IllegalStateException("Method should not be called when unpositioned");
        }
    }

    @Override
    public BytesRef term() throws IOException {
        ensurePositioned();
        return iterator.syntheticId();
    }

    @Override
    public BytesRef next() throws IOException {
        int nextDoc = iterator.nextDoc();

        // No more docs (or first time next() is called)
        if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
            int nextTsIdOrdinal = iterator.tsIdOrdinal() + 1;
            if (nextTsIdOrdinal == iterator.tsIdOrdinalMax()) {
                return null; // No more _tsid
            }
            this.iterator = createIterator(nextTsIdOrdinal, null);
            assert iterator.tsIdOrdinal() == nextTsIdOrdinal;
            nextDoc = iterator.nextDoc();
        }
        if (nextDoc != DocIdSetIterator.NO_MORE_DOCS) {
            return iterator.syntheticId();
        }
        return null;
    }

    @Override
    public SeekStatus seekCeil(BytesRef id) throws IOException {
        assert id != null;
        assert Long.BYTES < id.length : id.length;

        if (id == null || id.length <= Long.BYTES) {
            return SeekStatus.END;
        }

        // _id synthetic format = [timestamp(long), tsid]
        final byte[] idAsBytes = id.bytes;
        byte[] tsId = new byte[Math.toIntExact(id.length - Long.BYTES)];
        System.arraycopy(idAsBytes, Long.BYTES, tsId, 0, tsId.length);

        int lookupedTsIdOrd = iterator.lookupByTsId(new BytesRef(tsId));
        if (lookupedTsIdOrd >= 0) {
            long timestamp = ByteUtils.readLongBE(idAsBytes, 0);
            this.iterator = createIterator(lookupedTsIdOrd, timestamp);
            return iterator.hasExactTimestamp() ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;

        } else {
            int nextTsIdOrdinal = -lookupedTsIdOrd - 1;
            if (nextTsIdOrdinal == iterator.tsIdOrdinalMax()) {
                this.iterator = createEmptyIterator();
                return SeekStatus.END;
            } else {
                this.iterator = createIterator(nextTsIdOrdinal, null);
                return SeekStatus.NOT_FOUND;
            }
        }
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        ensurePositioned();
        return new SyntheticIdPostingsEnum(iterator);
    }

    @Override
    public int docFreq() {
        ensurePositioned();
        return 1; // This is not true, but that makes search working
    }

    @Override
    public long totalTermFreq() {
        return 1L; // This is not true, but that makes search working
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        return null;
    }

    private SyntheticDocIdSetIterator createEmptyIterator() throws IOException {
        return createIterator(-1, null);
    }

    private SyntheticDocIdSetIterator createIterator(int tsIdOrdinal, @Nullable Long timestamp) throws IOException {
        assert tsIds != null;
        assert tsIdOrdinal < tsIds.getValueCount();
        assert timestamps != null;

        int startDoc = -1;  // inclusive
        int endDoc = -1;    // exclusive

        if (0 <= tsIdOrdinal) {
            assert tsIdOrdinal < tsIds.getValueCount() : tsIdOrdinal;
            // Lookup the tsid term
            final BytesRef tsIdTerm = tsIds.lookupOrd(tsIdOrdinal);

            // Find the range of documents that match the given _tsid. Since _tsid's define
            // the primary ordering for the documents stored in a segment, we can just skip
            // documents whose _tsid ordinal is below or beyond targetTsidOrd.

            // This could take advantage of the doc value skippers to do a more efficient
            // docId search, but these are not exposed in a public API yet.
            boolean found = false;
            for (int docId = tsIds.nextDoc(); docId != NO_MORE_DOCS; docId = tsIds.nextDoc()) {
                int ord = tsIds.ordValue();
                if (ord < tsIdOrdinal) {
                    continue;
                }
                if (ord == tsIdOrdinal) {
                    if (startDoc == -1) {
                        startDoc = docId;
                    }
                    endDoc = docId + 1;
                    found = true;
                }
                if (ord > tsIdOrdinal) {
                    break;
                }
            }
            assert found : "Unknown _tsid ordinal: " + tsIdOrdinal + " " + tsIdTerm;
            assert 0 <= endDoc : endDoc;
            assert 0 <= startDoc : startDoc;

            // We should always have a valid _tsid ordinal but just in case
            if (found) {
                final SyntheticDocIdSetIterator syntheticDocIds;

                // Reduce the doc iterator to only have docs matching the timestamp
                boolean hasExactTimestamp = false;
                if (timestamp != null) {
                    var it = DocIdSetIterator.range(startDoc, endDoc);
                    startDoc = endDoc;
                    for (int docId = it.nextDoc(); docId != NO_MORE_DOCS; docId = it.nextDoc()) {
                        if (timestamps.advanceExact(docId)) {
                            long docTimestamp = timestamps.nextValue();
                            if (docTimestamp == timestamp) {
                                startDoc = Math.min(startDoc, docId);
                                hasExactTimestamp = true;
                            } else if (docTimestamp < timestamp) {
                                endDoc = docId;
                                break;
                            }
                        }
                    }
                    syntheticDocIds = new SyntheticDocIdSetIterator(
                        tsIds,
                        timestamps,
                        tsIdOrdinal,
                        tsIdTerm,
                        hasExactTimestamp ? DocIdSetIterator.range(startDoc, endDoc) : DocIdSetIterator.empty(),
                        hasExactTimestamp ? timestamp : null
                    );
                } else {
                    syntheticDocIds = new SyntheticDocIdSetIterator(
                        tsIds,
                        timestamps,
                        tsIdOrdinal,
                        tsIdTerm,
                        DocIdSetIterator.range(startDoc, endDoc),
                        hasExactTimestamp ? timestamp : null
                    );
                }
                // Assume iterator is positioned
                syntheticDocIds.nextDoc();
                return syntheticDocIds;
            }
        }

        // Return empty iterator
        return new SyntheticDocIdSetIterator(tsIds, timestamps, -1, null, DocIdSetIterator.empty(), null);
    }

    /**
     * {@link DocIdSetIterator} over all the documents matching the same _tsid value. Depending of the minDoc/maxDoc values, the set of
     * documents can be limited to documents also matching the same timestamp value.
     */
    private static class SyntheticDocIdSetIterator extends DocIdSetIterator {

        private final SortedDocValues tsIds;
        private final SortedNumericDocValues timestamps;

        private final int tsIdOrdinal;
        private final @Nullable BytesRef tsId;
        private final @Nullable Long timestamp;

        private final DocIdSetIterator docIds;

        private SyntheticDocIdSetIterator(
            SortedDocValues tsIds,
            SortedNumericDocValues timestamps,
            int tsIdOrdinal,
            @Nullable BytesRef tsId,
            DocIdSetIterator docIds,
            @Nullable Long timestamp
        ) {
            this.tsIds = tsIds;
            this.tsId = tsId;
            this.timestamps = timestamps;
            this.tsIdOrdinal = tsIdOrdinal;
            this.docIds = docIds;
            this.timestamp = timestamp;
        }

        public int tsIdOrdinal() {
            return tsIdOrdinal;
        }

        public int tsIdOrdinalMax() {
            return tsIds.getValueCount();
        }

        private BytesRef syntheticId() throws IOException {
            assert tsId != null;
            int target = docID();
            assert target != -1 : target;
            assert target != NO_MORE_DOCS : target;

            if (timestamp != null) {
                return id(tsId, timestamp);
            } else {
                var advance = timestamps.advanceExact(target);
                assert advance : "Doc has no timestamp doc value: " + target;
                return id(tsId, timestamps.nextValue());
            }
        }

        private int lookupByTsId(BytesRef tsId) throws IOException {
            return tsIds.lookupTerm(tsId);
        }

        private boolean hasExactTimestamp() {
            return timestamp != null;
        }

        @Override
        public int docID() {
            return docIds.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return docIds.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return docIds.advance(target);
        }

        @Override
        public long cost() {
            return docIds.cost();
        }
    }

    private static class SyntheticIdPostingsEnum extends PostingsEnum {

        private final SyntheticDocIdSetIterator delegate;
        private boolean positioned = false;

        private SyntheticIdPostingsEnum(SyntheticDocIdSetIterator delegate) {
            this.delegate = delegate;
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            if (positioned == false) {
                positioned = true;
                if (delegate.docID() >= 0) {
                    return delegate.docID();
                }
            }
            return delegate.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return delegate.advance(target);
        }

        @Override
        public long cost() {
            return delegate.cost();
        }

        @Override
        public int freq() throws IOException {
            return 0; // not supported
        }

        @Override
        public int nextPosition() throws IOException {
            return -1; // not supported
        }

        @Override
        public int startOffset() throws IOException {
            return -1; // not supported
        }

        @Override
        public int endOffset() throws IOException {
            return -1; // not supported
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return null; // not supported
        }
    }

    private static BytesRef id(BytesRef tsId, long timestamp) {
        assert tsId != null;
        byte[] id = new byte[tsId.length + Long.BYTES];
        ByteUtils.writeLongBE(timestamp, id, 0);   // Big Ending shrinks the inverted index by ~37%
        System.arraycopy(tsId.bytes, 0, id, Long.BYTES, tsId.length);
        return new BytesRef(id);
    }

    private static UnsupportedOperationException unsupportedException() {
        var error = "method should not be called on this enum";
        assert false : error;
        return new UnsupportedOperationException(error);
    }
}
