/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;

public final class FetchSourceSubPhase implements FetchSubPhase {

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.sourceRequested() == false) {
            return;
        }
        SourceLookup sourceLookup = context.lookup().source();
        FetchSourceContext fetchSourceContext = context.fetchSourceContext();
        assert fetchSourceContext.fetchSource();
        if (fetchSourceContext.isFiltered() == false) {
            hitContext.hit().sourceRef(sourceLookup.internalSourceRef());
            return;
        }

        if (sourceLookup.internalSourceRef() == null) {
            throw new IllegalArgumentException("unable to fetch fields from _source field: _source is disabled in the mappings " +
                    "for index [" + context.indexShard().shardId().getIndexName() + "]");
        }

        try {
            hitContext.hit().sourceRef(sourceLookup.filter(fetchSourceContext));
        } catch (IOException e) {
            throw new ElasticsearchException("Error filtering source", e);
        }
    }
}
