/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.IOException;

public class SyntheticIdLeafReader extends FilterLeafReader {

    public SyntheticIdLeafReader(LeafReader reader) {
        super(reader);
    }

    @Override
    public Terms terms(String field) throws IOException {
        if (IdFieldMapper.NAME.equals(field)) { // TODO is that necessary if we have a placeholder value for _id?
            return SyntheticIdTerms.from(getDelegate());
        }
        return super.terms(field);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return getDelegate().getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return getDelegate().getReaderCacheHelper();
    }
}
