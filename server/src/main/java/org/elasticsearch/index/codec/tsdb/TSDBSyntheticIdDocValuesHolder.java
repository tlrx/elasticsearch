/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;

/**
 * Holds all the doc values used in the {@link TSDBSyntheticIdFieldsProducer.SyntheticIdTermsEnum} and
 * {@link TSDBSyntheticIdFieldsProducer.SyntheticIdPostingsEnum} to lookup and to build synthetic _ids,
 * along with some utility methods to access doc values.
 * <p>
 * It holds the instance of {@link DocValuesProducer} used to create the sorted doc values for _tsid, @timestamp and
 * _ts_routing_hash. Because doc values can only advance, they are re-created from the {@link DocValuesProducer} when we need to
 * seek backward.
 * </p>
 */
class TSDBSyntheticIdDocValuesHolder {

    private final FieldInfo tsIdFieldInfo;
    private final FieldInfo timestampFieldInfo;
    private final FieldInfo routingHashFieldInfo;
    private final DocValuesProducer docValuesProducer;

    private SortedNumericDocValues timestampDocValues; // sorted desc. order
    private SortedDocValues routingHashDocValues; // sorted asc. order
    private SortedDocValues tsIdDocValues; // sorted asc. order
    // Keep around the latest tsId ordinal and value
    private int cachedTsIdOrd = -1;
    private BytesRef cachedTsId;

    TSDBSyntheticIdDocValuesHolder(FieldInfos fieldInfos, DocValuesProducer docValuesProducer) {
        this.tsIdFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TS_ID);
        this.timestampFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TIMESTAMP);
        this.routingHashFieldInfo = safeFieldInfo(fieldInfos, TSDBSyntheticIdPostingsFormat.TS_ROUTING_HASH);
        this.docValuesProducer = docValuesProducer;
    }

    private FieldInfo safeFieldInfo(FieldInfos fieldInfos, String fieldName) {
        var fi = fieldInfos.fieldInfo(fieldName);
        if (fi == null) {
            var message = "Field [" + fieldName + "] does not exist";
            assert false : message;
            throw new IllegalArgumentException(message);
        }
        return fi;
    }

    /**
     * Returns the _tsid ordinal value for a given docID. The document ID must exist and must have a value for the field.
     *
     * @param docID the docID
     * @return the _tsid ordinal value
     * @throws IOException if any I/O exception occurs
     */
    int docTsIdOrdinal(int docID) throws IOException {
        if (tsIdDocValues == null || tsIdDocValues.docID() > docID) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        boolean found = tsIdDocValues.advanceExact(docID);
        assert found : "No value found for field [" + tsIdFieldInfo.getName() + " and docID " + docID;
        return tsIdDocValues.ordValue();
    }

    /**
     * Returns the timestamp value for a given docID. The document ID must exist and must have a value for the field.
     *
     * @param docID the docID
     * @return the timestamp value
     * @throws IOException if any I/O exception occurs
     */
    long docTimestamp(int docID) throws IOException {
        if (timestampDocValues == null || timestampDocValues.docID() > docID) {
            timestampDocValues = docValuesProducer.getSortedNumeric(timestampFieldInfo);
        }
        boolean found = timestampDocValues.advanceExact(docID);
        assert found : "No value found for field [" + timestampFieldInfo.getName() + " and docID " + docID;
        assert timestampDocValues.docValueCount() == 1;
        return timestampDocValues.nextValue();
    }

    /**
     * Returns the routing hash value for a given docID. The document ID must exist and must have a value for the field.
     *
     * @param docID the docID
     * @return the routing hash value
     * @throws IOException if any I/O exception occurs
     */
    BytesRef docRoutingHash(int docID) throws IOException {
        if (routingHashDocValues == null || routingHashDocValues.docID() > docID) {
            routingHashDocValues = docValuesProducer.getSorted(routingHashFieldInfo);
        }
        boolean found = routingHashDocValues.advanceExact(docID);
        assert found : "No value found for field [" + routingHashFieldInfo.getName() + " and docID " + docID;
        return routingHashDocValues.lookupOrd(routingHashDocValues.ordValue());
    }

    /**
     * Lookup if a given _tsid exists, returning a positive ordinal if it exists otherwise it returns -insertionPoint-1.
     *
     * @param tsId the _tsid to look up
     * @return a positive ordinal if the _tsid exists, else returns -insertionPoint-1.
     * @throws IOException if any I/O exception occurs
     */
    int lookupTsIdTerm(BytesRef tsId) throws IOException {
        int compare = Integer.MAX_VALUE;
        if (cachedTsId != null) {
            compare = cachedTsId.compareTo(tsId);
            if (compare == 0) {
                return cachedTsIdOrd;
            }
        }
        if (tsIdDocValues == null || compare > 0) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        int ordinal = tsIdDocValues.lookupTerm(tsId);
        if (0 <= ordinal) {
            cachedTsIdOrd = ordinal;
            cachedTsId = tsId;
        }
        return ordinal;
    }

    /**
     * Lookup the _tsid value for the given ordinal.
     *
     * @param tsIdOrdinal the _tsid  ordinal
     * @return the _tsid value
     * @throws IOException if any I/O exception occurs
     */
    BytesRef lookupTsIdOrd(int tsIdOrdinal) throws IOException {
        if (cachedTsIdOrd != -1 && cachedTsIdOrd == tsIdOrdinal) {
            return cachedTsId;
        }
        if (tsIdDocValues == null || tsIdDocValues.ordValue() > tsIdOrdinal) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        assert 0 <= tsIdOrdinal : tsIdOrdinal;
        assert tsIdOrdinal < tsIdDocValues.getValueCount() : tsIdOrdinal;
        var tsId = tsIdDocValues.lookupOrd(tsIdOrdinal);
        if (tsId != null) {
            cachedTsIdOrd = tsIdOrdinal;
            cachedTsId = tsId;
        }
        return tsId;
    }

    /**
     * Scan all documents to find the first document that has a _tsid equal or greater than the provided _tsid ordinal, returning its
     * document ID. If no document is found, the method returns {@link DocIdSetIterator#NO_MORE_DOCS}.
     * <p>
     * Warning: This method is very slow because it potentially scans all documents in the segment.
     */
    int slowScanToFirstDocWithTsIdOrdinalEqualOrGreaterThan(int tsIdOrd) throws IOException {
        // recreate even if doc values are already on the same ordinal, to ensure the method returns the first doc
        if (tsIdDocValues == null || (cachedTsIdOrd != -1 && cachedTsIdOrd >= tsIdOrd)) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        assert 0 <= tsIdOrd : tsIdOrd;
        assert tsIdOrd < tsIdDocValues.getValueCount() : tsIdOrd;

        for (int docID = 0; docID != DocIdSetIterator.NO_MORE_DOCS; docID = tsIdDocValues.nextDoc()) {
            boolean found = tsIdDocValues.advanceExact(docID);
            assert found : "No value found for field [" + tsIdFieldInfo.getName() + " and docID " + docID;
            var ord = tsIdDocValues.ordValue();
            if (ord == tsIdOrd || tsIdOrd < ord) {
                if (ord != cachedTsIdOrd) {
                    cachedTsId = tsIdDocValues.lookupOrd(ord);
                    cachedTsIdOrd = ord;
                }
                return docID;
            }
        }
        cachedTsIdOrd = -1;
        cachedTsId = null;
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    /**
     * Scan all documents to find the first document that has a _tsid equal to the provided _tsid ordinal, returning its
     * document ID. If no document is found, the method returns {@link DocIdSetIterator#NO_MORE_DOCS}.
     * <p>
     * Warning: This method is very slow because it potentially scans all documents in the segment.
     */
    int slowScanToFirstDocWithTsIdOrdinalEqualTo(int tsIdOrd) throws IOException {
        // recreate even if doc values are already on the same ordinal, to ensure the method returns the first doc
        if (tsIdDocValues == null || (cachedTsIdOrd != -1 && cachedTsIdOrd >= tsIdOrd)) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
            cachedTsIdOrd = -1;
            cachedTsId = null;
        }
        assert 0 <= tsIdOrd : tsIdOrd;
        assert tsIdOrd < tsIdDocValues.getValueCount() : tsIdOrd;

        for (int docID = 0; docID != DocIdSetIterator.NO_MORE_DOCS; docID = tsIdDocValues.nextDoc()) {
            boolean found = tsIdDocValues.advanceExact(docID);
            assert found : "No value found for field [" + tsIdFieldInfo.getName() + " and docID " + docID;
            var ord = tsIdDocValues.ordValue();
            if (ord == tsIdOrd) {
                if (ord != cachedTsIdOrd) {
                    cachedTsId = tsIdDocValues.lookupOrd(ord);
                    cachedTsIdOrd = ord;
                }
                return docID;
            } else if (tsIdOrd < ord) {
                break;
            }
        }
        cachedTsIdOrd = -1;
        cachedTsId = null;
        assert false : "Method must be called with an existing _tsid ordinal: " + tsIdOrd;
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    int getTsIdValueCount() throws IOException {
        if (tsIdDocValues == null) {
            tsIdDocValues = docValuesProducer.getSorted(tsIdFieldInfo);
        }
        return tsIdDocValues.getValueCount();
    }

    /**
     * Returns the synthetic _id for a given document ID. Document must exist.
     *
     * @param docID the document ID
     * @return  the synthetic _id
     * @throws IOException if any I/O exception occurs
     */
    BytesRef docSyntheticId(int docID) throws IOException {
        return syntheticId(lookupTsIdOrd(docTsIdOrdinal(docID)), docTimestamp(docID), docRoutingHash(docID));
    }

    static BytesRef syntheticId(BytesRef tsId, long timestamp, BytesRef routingHashBytes) {
        assert tsId != null;
        assert timestamp > 0L;
        assert routingHashBytes != null;
        String routingHashString = Uid.decodeId(routingHashBytes.bytes, routingHashBytes.offset, routingHashBytes.length);
        int routingHash = TimeSeriesRoutingHashFieldMapper.decode(routingHashString);
        return TsidExtractingIdFieldMapper.createSyntheticIdBytesRef(tsId, timestamp, routingHash);
    }
}
