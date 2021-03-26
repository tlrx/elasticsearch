/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store;

import org.elasticsearch.common.util.concurrent.SearchThread;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class ProfilingSearchOperationListener implements SearchOperationListener {

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        onPhase(profilingMap -> {
            if (searchContext.getProfilers() != null) {
                final QuerySearchResult queryResult = searchContext.queryResult();
                if (queryResult.hasProfileResults()) {
                    final ProfileShardResult profileShardResult = queryResult.consumeProfileResult();
                    searchContext.queryResult().profileResults(buildSearchableSnapshotsProfileResult(profilingMap, profileShardResult));
                }
            }
        });
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        onPhase(profilingMap -> {
            if (searchContext.getProfilers() != null) {
                System.out.println();
            }
        });
    }

    private void onPhase(Consumer<Map<String, Object>> consumer) {
        final Thread currentThread = Thread.currentThread();
        if (currentThread instanceof SearchThread) {
            final Map<String, Object> profilingMap = ((SearchThread) currentThread).getProfilingMap();
            assert profilingMap != null;
            consumer.accept(profilingMap);
        }
    }

    private ProfileShardResult buildSearchableSnapshotsProfileResult(Map<String, Object> profilingMap, ProfileShardResult original) {
        final List<QueryProfileShardResult> queryProfileResults = original.getQueryProfileResults();

        final List<QueryProfileShardResult> updated = new ArrayList<>();
        for (int i = 0; i < queryProfileResults.size(); i++) {
            QueryProfileShardResult result = queryProfileResults.get(i);
            if (i == 0) {
                List<ProfileResult> profileResults = new ArrayList<>(result.getQueryResults());
                profileResults.add(new ProfileResult(
                    "searchable_snapshots",
                    "description",
                    buildBreakdown(profilingMap),
                    emptyMap(),
                    0L,
                    emptyList()
                ));
                updated.add(new QueryProfileShardResult(profileResults, result.getRewriteTime(), result.getCollectorResult()));
            } else {
                updated.add(result);
            }
        }
        return new ProfileShardResult(updated, original.getAggregationProfileResults());
    }

    private Map<String, Long> buildBreakdown(Map<String, Object> profilingMap) {
        final Map<String, Long> map = new HashMap<>();
        map.put("lucene_bytes_read",
            profilingMap.values().stream().map(o -> (IndexInputStats) o).mapToLong(stat -> stat.getLuceneBytesRead().total()).sum());
        map.put("cached_bytes_read",
            profilingMap.values().stream().map(o -> (IndexInputStats) o).mapToLong(stat -> stat.getCachedBytesRead().total()).sum());
        map.put("cached_bytes_written",
            profilingMap.values().stream().map(o -> (IndexInputStats) o).mapToLong(stat -> stat.getCachedBytesWritten().total()).sum());
        map.put("direct_bytes_read",
            profilingMap.values().stream().map(o -> (IndexInputStats) o).mapToLong(stat -> stat.getDirectBytesRead().total()).sum());
        map.put("blob_store_bytes_requested",
            profilingMap.values().stream().map(o -> (IndexInputStats) o).mapToLong(stat -> stat.getBlobStoreBytesRequested().total()).sum());
        return map;
    }
}
