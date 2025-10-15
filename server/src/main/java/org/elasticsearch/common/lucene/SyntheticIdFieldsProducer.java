/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class SyntheticIdFieldsProducer extends FieldsProducer {

    private final @Nullable FieldInfo tsIdField;
    private final @Nullable FieldInfo timestampField;
    private final DocValuesProducer docValuesProducer;
    private final int maxDocs;

    public SyntheticIdFieldsProducer(SegmentReadState state, DocValuesProducer docValuesProducer) {
        var tsIdField = state.fieldInfos.fieldInfo(TimeSeriesIdFieldMapper.NAME);
        var timestampField = state.fieldInfos.fieldInfo("@timestamp");
        if (tsIdField != null && timestampField != null) {
            this.tsIdField = tsIdField;
            this.timestampField = timestampField;
        } else {
            this.tsIdField = null;
            this.timestampField = null;
        }
        this.maxDocs = state.segmentInfo.maxDoc();
        this.docValuesProducer = docValuesProducer;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(docValuesProducer);
    }

    @Override
    public void checkIntegrity() throws IOException {}

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Iterator<String> iterator() {
        return List.of(IdFieldMapper.NAME).iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        assert IdFieldMapper.NAME.equals(field) : field;

        SortedDocValues tsIds = null;
        if (tsIdField != null) {
            tsIds = docValuesProducer.getSorted(tsIdField);
            if (tsIds == null) {
                throw new IllegalStateException();
            }
        }
        SortedNumericDocValues timestamps = null;
        if (tsIdField != null) {
            timestamps = docValuesProducer.getSortedNumeric(timestampField);
            if (timestamps == null) {
                throw new IllegalStateException();
            }
        }
        if (tsIds == null && timestamps == null) {
            return null;
        }
        return SyntheticIdTerms.from(tsIds, timestamps, maxDocs);
    }
}
