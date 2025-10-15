/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;
import java.util.Objects;

public class SyntheticIdTerms extends Terms {

    private static final SyntheticIdTerms EMPTY = new SyntheticIdTerms(DocValues.emptySorted(), DocValues.emptySortedNumeric(), 1);

    private final SortedDocValues tsIds;// sorted asc. order
    private final SortedNumericDocValues timestamps;  // sorted desc. order
    private final int maxDocs;

    private SyntheticIdTerms(SortedDocValues tsIdsDocValues, SortedNumericDocValues timestampsDocValues, int maxDocs) {
        this.tsIds = Objects.requireNonNull(tsIdsDocValues);
        this.timestamps = Objects.requireNonNull(timestampsDocValues);
        this.maxDocs = maxDocs;
    }

    @Override
    public TermsEnum iterator() throws IOException {
        return new SyntheticIdTermsEnum(tsIds, timestamps);
    }

    @Override
    public int getDocCount() throws IOException {
        return maxDocs - 1; // All docs have a synthetic id
    }

    @Override
    public long size() throws IOException {
        return -1; // Number of terms unknown
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
        return 0;
    }

    @Override
    public long getSumDocFreq() throws IOException {
        return 0;
    }

    @Override
    public boolean hasFreqs() {
        return false;
    }

    @Override
    public boolean hasOffsets() {
        return false;
    }

    @Override
    public boolean hasPositions() {
        return false;
    }

    @Override
    public boolean hasPayloads() {
        return false;
    }

    private static SyntheticIdTerms empty() {
        return EMPTY;
    }

    public static SyntheticIdTerms from(SortedDocValues tsIds, SortedNumericDocValues timestamps, int maxDocs) {
        assert (tsIds != null && timestamps != null) || (tsIds == null && timestamps == null);
        if (tsIds != null && timestamps != null) {
            return new SyntheticIdTerms(tsIds, timestamps, maxDocs);
        }
        return empty();
    }

    public static SyntheticIdTerms from(LeafReader reader) throws IOException {
        return from(
            reader.getSortedDocValues(TimeSeriesIdFieldMapper.NAME),
            reader.getSortedNumericDocValues("@timestamp"),
            reader.maxDoc()
        );
    }
}
