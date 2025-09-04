/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.uid;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.codec.bloomfilter.BloomFilterSettings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/** Utility class to do efficient primary-key (only 1 doc contains the
 *  given term) lookups by segment, re-using the enums.  This class is
 *  not thread safe, so it is the caller's job to create and use one
 *  instance of this per thread.  Do not use this if a term may appear
 *  in more than one document!  It will only return the first one it
 *  finds.
 *  This class uses live docs, so it should be cached based on the
 *  {@link org.apache.lucene.index.IndexReader#getReaderCacheHelper() reader cache helper}
 *  rather than the {@link LeafReader#getCoreCacheHelper() core cache helper}.
 */
final class PerThreadIDVersionAndSeqNoLookup {
    // TODO: do we really need to store all this stuff? some if it might not speed up anything.
    // we keep it around for now, to reduce the amount of e.g. hash lookups by field and stuff

    private final TermsEnum termsEnum;

    /** Reused for iteration (when the term exists) */
    private PostingsEnum docsEnum;

    /** used for assertions to make sure class usage meets assumptions */
    private final Object readerKey;

    final boolean loadedTimestampRange;
    final long minTimestamp;
    final long maxTimestamp;

    /**
     * Initialize lookup for the provided segment
     */
    PerThreadIDVersionAndSeqNoLookup(LeafReader reader, boolean trackReaderKey, boolean loadTimestampRange) throws IOException {
        final Terms terms = reader.terms(IdFieldMapper.NAME);
        if (terms == null) {
            // If a segment contains only no-ops, it does not have _uid but has both _soft_deletes and _tombstone fields.
            final NumericDocValues softDeletesDV = reader.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
            final NumericDocValues tombstoneDV = reader.getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
            // this is a special case when we pruned away all IDs in a segment since all docs are deleted.
            final boolean allDocsDeleted = (softDeletesDV != null && reader.numDocs() == 0);
            if ((softDeletesDV == null || tombstoneDV == null) && allDocsDeleted == false) {
                throw new IllegalArgumentException(
                    "reader does not have _uid terms but not a no-op segment; "
                        + "_soft_deletes ["
                        + softDeletesDV
                        + "], _tombstone ["
                        + tombstoneDV
                        + "]"
                );
            }
            termsEnum = null;
        } else {
            termsEnum = terms.iterator();
        }
        if (reader.getNumericDocValues(VersionFieldMapper.NAME) == null) {
            throw new IllegalArgumentException("reader misses the [" + VersionFieldMapper.NAME + "] field; _uid terms [" + terms + "]");
        }
        Object readerKey = null;
        assert trackReaderKey ? (readerKey = reader.getCoreCacheHelper().getKey()) != null : readerKey == null;
        this.readerKey = readerKey;

        this.loadedTimestampRange = loadTimestampRange;
        // Also check for the existence of the timestamp field, because sometimes a segment can only contain tombstone documents,
        // which don't have any mapped fields (also not the timestamp field) and just some meta fields like _id, _seq_no etc.
        long minTimestamp = 0;
        long maxTimestamp = Long.MAX_VALUE;
        if (loadTimestampRange) {
            FieldInfo info = reader.getFieldInfos().fieldInfo(DataStream.TIMESTAMP_FIELD_NAME);
            if (info != null) {
                if (info.docValuesSkipIndexType() == DocValuesSkipIndexType.RANGE) {
                    DocValuesSkipper skipper = reader.getDocValuesSkipper(DataStream.TIMESTAMP_FIELD_NAME);
                    assert skipper != null : "no skipper for reader:" + reader + " and parent:" + reader.getContext().parent.reader();
                    minTimestamp = skipper.minValue();
                    maxTimestamp = skipper.maxValue();
                } else {
                    PointValues tsPointValues = reader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
                    assert tsPointValues != null
                        : "no timestamp field for reader:" + reader + " and parent:" + reader.getContext().parent.reader();
                    minTimestamp = LongPoint.decodeDimension(tsPointValues.getMinPackedValue(), 0);
                    maxTimestamp = LongPoint.decodeDimension(tsPointValues.getMaxPackedValue(), 0);
                }
            }
        }
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    PerThreadIDVersionAndSeqNoLookup(LeafReader reader, boolean loadTimestampRange) throws IOException {
        this(reader, true, loadTimestampRange);
    }

    /** Return null if id is not found.
     * We pass the {@link LeafReaderContext} as an argument so that things
     * still work with reader wrappers that hide some documents while still
     * using the same cache key. Otherwise we'd have to disable caching
     * entirely for these readers.
     */
    public DocIdAndVersion lookupVersion(BytesRef id, boolean loadSeqNo, LeafReaderContext context) throws IOException {
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";
        int docID = getDocID(id, context);

        return getDocIdAndVersionForDoc(loadSeqNo, context, docID);
    }

    public DocIdAndVersion lookupVersionWithTsIdAndTimestamp(
        BytesRef id,
        BytesRef tsId,
        long timestamp,
        boolean loadSeqNo,
        LeafReaderContext context
    ) throws IOException {
        int docID = getDocIDForTsIdAndTimestamp(id, tsId, timestamp, context);

        return getDocIdAndVersionForDoc(loadSeqNo, context, docID);
    }

    public int getDocIDForTsIdAndTimestamp(BytesRef id, BytesRef tsId, long timestamp, LeafReaderContext context) throws IOException {
        if (BloomFilterSettings.SKIP_LOOKUP.get()) {
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        // First check the bloom filter for the _id
        if (termsEnum != null && (BloomFilterSettings.FORCE_LOOKUP.get() || termsEnum.seekExact(id))) {
            var tsIds = context.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME); // sorted ascending order
            // Always a singleton field and sorted descending order:
            var timestamps = DocValues.unwrapSingleton(context.reader().getSortedNumericDocValues("@timestamp"));

            int targetTsidOrd = tsIds.lookupTerm(tsId);
            var liveDocs = context.reader().getLiveDocs();
            if (targetTsidOrd > 0) {
                int startDocId = -1;
                int endDocId = -1;
                int maxDocId = context.reader().maxDoc();
                // Find the range of documents that match the given _tsid. Since _tsid's define
                // the primary ordering for the documents stored in a segment, we can just skip
                // documents whose _tsid ordinal is below or beyond targetTsidOrd.

                // This could take advantage of the doc value skippers to do a more efficient
                // docId search, but these are not exposed in a public API yet.
                for (int docId = tsIds.nextDoc(); docId < maxDocId; docId = tsIds.nextDoc()) {
                    int tsidOrd = tsIds.ordValue();
                    if (tsidOrd < targetTsidOrd) {
                        continue;
                    }
                    if (tsidOrd == targetTsidOrd && startDocId == -1) {
                        startDocId = docId;
                    }
                    if (tsidOrd > targetTsidOrd || docId == maxDocId - 1) {
                        if (startDocId != -1) {
                            endDocId = docId;
                            break;
                        } else {
                            return DocIdSetIterator.NO_MORE_DOCS;
                        }
                    }
                }
                for (int docId = startDocId; docId <= endDocId; docId++) {
                    if (timestamps.advanceExact(docId)) {
                        long docTimestamp = timestamps.longValue();
                        if (docTimestamp == timestamp) {
                            if (liveDocs == null || liveDocs.get(docId)) {
                                return docId;
                            }
                        } else if (docTimestamp < timestamp) {
                            return DocIdSetIterator.NO_MORE_DOCS;
                        }
                    }
                }
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    /**
     * returns the internal lucene doc id for the given id bytes.
     * {@link DocIdSetIterator#NO_MORE_DOCS} is returned if not found
     * */
    private int getDocID(BytesRef id, LeafReaderContext context) throws IOException {
        // TODO: avoid doing the parsing too often
        // _id synthetic format = [timestamp(long), tsid]
        byte[] idAsBytes = id.bytes;
        long timestamp = ByteUtils.readLongBE(idAsBytes, 0);
        byte[] tsId = new byte[idAsBytes.length - Long.BYTES];
        System.arraycopy(idAsBytes, Long.BYTES, tsId, 0, tsId.length);

        return getDocIDForTsIdAndTimestamp(id, new BytesRef(tsId), timestamp, context);
    }

    private static DocIdAndVersion getDocIdAndVersionForDoc(boolean loadSeqNo, LeafReaderContext context, int docID) throws IOException {
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final long seqNo;
            final long term;
            if (loadSeqNo) {
                seqNo = readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID);
                term = readNumericDocValues(context.reader(), SeqNoFieldMapper.PRIMARY_TERM_NAME, docID);
            } else {
                seqNo = UNASSIGNED_SEQ_NO;
                term = UNASSIGNED_PRIMARY_TERM;
            }
            final long version = readNumericDocValues(context.reader(), VersionFieldMapper.NAME, docID);
            return new DocIdAndVersion(docID, version, seqNo, term, context.reader(), context.docBase);
        } else {
            return null;
        }
    }

    private static long readNumericDocValues(LeafReader reader, String field, int docId) throws IOException {
        final NumericDocValues dv = reader.getNumericDocValues(field);
        if (dv == null || dv.advanceExact(docId) == false) {
            assert false : "document [" + docId + "] does not have docValues for [" + field + "]";
            throw new IllegalStateException("document [" + docId + "] does not have docValues for [" + field + "]");
        }
        return dv.longValue();
    }

    /** Return null if id is not found. */
    DocIdAndSeqNo lookupSeqNo(BytesRef id, LeafReaderContext context) throws IOException {
        assert readerKey == null || context.reader().getCoreCacheHelper().getKey().equals(readerKey)
            : "context's reader is not the same as the reader class was initialized on.";
        final int docID = getDocID(id, context);
        if (docID != DocIdSetIterator.NO_MORE_DOCS) {
            final long seqNo = readNumericDocValues(context.reader(), SeqNoFieldMapper.NAME, docID);
            return new DocIdAndSeqNo(docID, seqNo, context);
        } else {
            return null;
        }
    }
}
