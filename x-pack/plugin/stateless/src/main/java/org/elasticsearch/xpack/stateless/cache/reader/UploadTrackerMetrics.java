/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.function.BooleanSupplier;

/// Metrics describing the reads of search shards that fail because the VBCC they target was released by the indexing shard:
///
/// - [#VBCC_NOT_FOUND_TOTAL_METRIC]: one sample per failed read, tagged with the [#CAUSE_ATTRIBUTE] explaining why the search shard was
///   still reading from the VBCC.
///
/// Samples are only recorded while the supplied `metricsEnabled` flag is true, which is expected to follow
/// `SearchCommitPrefetcherDynamicSettings#prefetchMetricsEnabled()`.
public record UploadTrackerMetrics(LongCounter vbccNotFoundCounter, BooleanSupplier metricsEnabled) {

    public static final String VBCC_NOT_FOUND_TOTAL_METRIC = "es.stateless.search.vbcc_not_found.total";

    /// The [VbccNotFoundCause#attributeValue()] of the failed read.
    public static final String CAUSE_ATTRIBUTE = "es_cause";

    public static final UploadTrackerMetrics NOOP = new UploadTrackerMetrics(
        MeterRegistry.NOOP.registerLongCounter(VBCC_NOT_FOUND_TOTAL_METRIC + ".noop", "noop", "count"),
        () -> false
    );

    /**
     * Why a read targeting a VBCC failed with a {@code ResourceNotFoundException}.
     */
    public enum VbccNotFoundCause {
        // The tracker already knows the BCC is uploaded: the read was in flight when the tracker was updated.
        ALREADY_UPDATED("already_updated"),
        // A notification announced the BCC as uploaded, but the tracker is not updated yet, i.e. its prefetch is still running.
        PREFETCH_PENDING("prefetch_pending"),
        // No notification announced the BCC as uploaded.
        NOTIFICATION_PENDING("notification_pending");

        private final String attributeValue;

        VbccNotFoundCause(String attributeValue) {
            this.attributeValue = attributeValue;
        }

        public String attributeValue() {
            return attributeValue;
        }
    }

    public static UploadTrackerMetrics register(MeterRegistry registry, BooleanSupplier metricsEnabled) {
        return new UploadTrackerMetrics(
            registry.registerLongCounter(
                VBCC_NOT_FOUND_TOTAL_METRIC,
                "Number of reads from a VBCC that failed because the VBCC was released by the indexing shard",
                "count"
            ),
            metricsEnabled
        );
    }

    public void recordVbccNotFound(VbccNotFoundCause cause) {
        if (metricsEnabled.getAsBoolean()) {
            vbccNotFoundCounter.incrementBy(1L, Map.of(CAUSE_ATTRIBUTE, cause.attributeValue()));
        }
    }
}
