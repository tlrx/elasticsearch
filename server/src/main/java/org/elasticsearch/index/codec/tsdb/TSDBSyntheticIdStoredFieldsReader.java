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
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SyntheticIdField;

import java.io.IOException;
import java.io.UncheckedIOException;

public class TSDBSyntheticIdStoredFieldsReader extends StoredFieldsReader {

    private final Directory directory;
    private final SegmentInfo si;
    private final FieldInfos fieldInfos;
    private final IOContext context;

    private final DocValuesProducer docValuesProducer;
    private final @Nullable TSDBSyntheticIdDocValuesHolder docValuesHolder; // null if no _id field or _id field is not synthetic
    private final @Nullable FieldInfo fieldInfo;                            // null if no _id field or _id field is not synthetic

    private TSDBSyntheticIdStoredFieldsReader(
        Directory directory,
        SegmentInfo si,
        FieldInfos fieldInfos,
        IOContext context,
        DocValuesProducer docValuesProducer
    ) {
        this.directory = directory;
        this.si = si;
        this.fieldInfos = fieldInfos;
        this.context = context;
        this.docValuesProducer = docValuesProducer;
        var idFieldInfo = idFieldInfo(fieldInfos);
        if (idFieldInfo != null) {
            this.docValuesHolder = new TSDBSyntheticIdDocValuesHolder(fieldInfos, docValuesProducer);
            this.fieldInfo = idFieldInfo;
        } else {
            this.docValuesHolder = null;
            this.fieldInfo = null;
        }
    }

    @Override
    public StoredFieldsReader clone() {
        try {
            return open(directory, si, fieldInfos, context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void checkIntegrity() throws IOException {}

    @Override
    public void close() throws IOException {
        IOUtils.close(docValuesProducer);
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        if (fieldInfo != null && docValuesHolder != null) {
            var id = docValuesHolder.docSyntheticId(docID);
            visitor.binaryField(fieldInfo, id.bytes);
        }
    }

    @Nullable
    private static FieldInfo idFieldInfo(FieldInfos fieldInfos) {
        var fieldInfo = fieldInfos.fieldInfo(IdFieldMapper.NAME);
        if (fieldInfo != null && SyntheticIdField.hasSyntheticIdAttributes(fieldInfo.attributes())) {
            return fieldInfo;
        }
        return null;
    }

    public static TSDBSyntheticIdStoredFieldsReader open(
        final Directory directory,
        final SegmentInfo si,
        final FieldInfos fn,
        final IOContext context
    ) throws IOException {
        DocValuesProducer docValuesProducer = null;
        boolean success = false;
        try {
            docValuesProducer = si.getCodec().docValuesFormat().fieldsProducer(new SegmentReadState(directory, si, fn, context));
            var storedFieldsReader = new TSDBSyntheticIdStoredFieldsReader(directory, si, fn, context, docValuesProducer);
            success = true;
            return storedFieldsReader;
        } finally {
            if (success == false) {
                IOUtils.close(docValuesProducer);
            }
        }
    }
}
