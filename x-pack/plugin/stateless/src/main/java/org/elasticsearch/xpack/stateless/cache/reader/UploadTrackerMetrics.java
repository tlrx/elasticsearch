/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.function.BooleanSupplier;

/// Metrics describing the reads of search shards that target a VBCC while the indexing shard may already have released it.
///
/// A window is the period between a commit notification announcing a BCC as uploaded and the end of the prefetch of that BCC. A read that
/// starts inside a window belongs to it, even if it completes after the prefetch is done.
///
/// Per read, recorded when the read completes:
///
/// - [#VBCC_NOT_FOUND_TOTAL_METRIC]: one sample per read that failed because the VBCC was released, tagged with the [#CAUSE_ATTRIBUTE]
///   explaining why the search shard was still reading from the VBCC.
/// - [#READS_IN_WINDOW_TOTAL_METRIC] and [#READ_BYTES_IN_WINDOW_TOTAL_METRIC]: the reads that started inside a window, and the bytes they
///   requested, tagged with the [#OUTCOME_ATTRIBUTE].
///
/// Per window, recorded once the prefetch is done and every read that started inside it has completed, and only if at least one read
/// started inside it:
///
/// - [#WINDOW_DURATION_HISTOGRAM_METRIC]: from the notification to the end of the prefetch.
/// - [#NOT_FOUND_TO_PREFETCH_DONE_HISTOGRAM_METRIC]: from the first failed read to the end of the prefetch, only if a read failed.
/// - [#READS_OK_HISTOGRAM_METRIC], [#READS_NOT_FOUND_HISTOGRAM_METRIC], [#BYTES_OK_HISTOGRAM_METRIC] and
///   [#BYTES_NOT_FOUND_HISTOGRAM_METRIC]: the reads and requested bytes by outcome.
///
/// Samples are only recorded while the supplied `metricsEnabled` flag is true, which is expected to follow
/// `SearchCommitPrefetcherDynamicSettings#prefetchMetricsEnabled()`.
public record UploadTrackerMetrics(
    LongCounter vbccNotFoundCounter,
    LongCounter readsInWindowCounter,
    LongCounter readBytesInWindowCounter,
    LongHistogram windowDurationHistogram,
    LongHistogram notFoundToPrefetchDoneHistogram,
    LongHistogram readsOkHistogram,
    LongHistogram readsNotFoundHistogram,
    LongHistogram bytesOkHistogram,
    LongHistogram bytesNotFoundHistogram,
    BooleanSupplier metricsEnabled
) {

    public static final String VBCC_NOT_FOUND_TOTAL_METRIC = "es.stateless.search.vbcc_not_found.total";
    public static final String READS_IN_WINDOW_TOTAL_METRIC = "es.stateless.search.vbcc_read_in_window.total";
    public static final String READ_BYTES_IN_WINDOW_TOTAL_METRIC = "es.stateless.search.vbcc_read_in_window.bytes.total";
    public static final String WINDOW_DURATION_HISTOGRAM_METRIC = "es.stateless.search.vbcc_window.duration.histogram";
    public static final String NOT_FOUND_TO_PREFETCH_DONE_HISTOGRAM_METRIC =
        "es.stateless.search.vbcc_not_found_to_prefetch_done.histogram";
    public static final String READS_OK_HISTOGRAM_METRIC = "es.stateless.search.vbcc_window.reads_ok.histogram";
    public static final String READS_NOT_FOUND_HISTOGRAM_METRIC = "es.stateless.search.vbcc_window.reads_not_found.histogram";
    public static final String BYTES_OK_HISTOGRAM_METRIC = "es.stateless.search.vbcc_window.bytes_ok.histogram";
    public static final String BYTES_NOT_FOUND_HISTOGRAM_METRIC = "es.stateless.search.vbcc_window.bytes_not_found.histogram";

    /// The [VbccNotFoundCause#attributeValue()] of the failed read.
    public static final String CAUSE_ATTRIBUTE = "es_cause";

    /// The [ReadOutcome#attributeValue()] of a read that started inside a window.
    public static final String OUTCOME_ATTRIBUTE = "outcome";

    public static final UploadTrackerMetrics NOOP = register(MeterRegistry.NOOP, () -> false);

    /**
     * Why a read targeting a VBCC failed with a {@code ResourceNotFoundException}.
     */
    public enum VbccNotFoundCause {
        // The read did not start inside a window, and the tracker already knows the BCC is uploaded: the read was in flight when the
        // tracker was updated by another read or by a prefetch.
        ALREADY_UPDATED("already_updated"),
        // The read started inside a window: a notification announced the BCC as uploaded, but its prefetch was not done yet. The read
        // may have failed after the prefetch was done.
        PREFETCH_PENDING("prefetch_pending"),
        // The read did not start inside a window, and no notification announced the BCC as uploaded.
        NOTIFICATION_PENDING("notification_pending");

        private final String attributeValue;

        VbccNotFoundCause(String attributeValue) {
            this.attributeValue = attributeValue;
        }

        public String attributeValue() {
            return attributeValue;
        }
    }

    /**
     * The outcome of a read that started inside a window.
     */
    public enum ReadOutcome {
        OK("ok"),
        NOT_FOUND("not_found");

        private final String attributeValue;

        ReadOutcome(String attributeValue) {
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
            registry.registerLongCounter(
                READS_IN_WINDOW_TOTAL_METRIC,
                "Number of reads from a VBCC that started between the notification of its upload and the end of its prefetch",
                "count"
            ),
            registry.registerLongCounter(
                READ_BYTES_IN_WINDOW_TOTAL_METRIC,
                "Bytes requested by the reads from a VBCC that started between the notification of its upload and the end of its prefetch",
                "bytes"
            ),
            registry.registerLongHistogram(
                WINDOW_DURATION_HISTOGRAM_METRIC,
                "Time between the notification of a BCC upload and the end of its prefetch, for windows with at least one read",
                "milliseconds"
            ),
            registry.registerLongHistogram(
                NOT_FOUND_TO_PREFETCH_DONE_HISTOGRAM_METRIC,
                "Time between the first read of a window that failed and the end of the prefetch",
                "milliseconds"
            ),
            registry.registerLongHistogram(READS_OK_HISTOGRAM_METRIC, "Number of reads of a window that succeeded", "count"),
            registry.registerLongHistogram(
                READS_NOT_FOUND_HISTOGRAM_METRIC,
                "Number of reads of a window that failed because the VBCC was released",
                "count"
            ),
            registry.registerLongHistogram(BYTES_OK_HISTOGRAM_METRIC, "Bytes requested by the reads of a window that succeeded", "bytes"),
            registry.registerLongHistogram(
                BYTES_NOT_FOUND_HISTOGRAM_METRIC,
                "Bytes requested by the reads of a window that failed because the VBCC was released",
                "bytes"
            ),
            metricsEnabled
        );
    }

    public boolean enabled() {
        return metricsEnabled.getAsBoolean();
    }

    public void recordVbccNotFound(VbccNotFoundCause cause) {
        if (enabled()) {
            vbccNotFoundCounter.incrementBy(1L, Map.of(CAUSE_ATTRIBUTE, cause.attributeValue()));
        }
    }

    public void recordReadInWindow(ReadOutcome outcome, long bytes) {
        if (enabled()) {
            final var attributes = Map.<String, Object>of(OUTCOME_ATTRIBUTE, outcome.attributeValue());
            readsInWindowCounter.incrementBy(1L, attributes);
            readBytesInWindowCounter.incrementBy(bytes, attributes);
        }
    }

    /**
     * Records the summary of a window.
     *
     * @param windowDurationMillis          time between the notification and the end of the prefetch
     * @param notFoundToPrefetchDoneMillis  time between the first failed read and the end of the prefetch, or a negative value if no read
     *                                      failed
     */
    public void recordWindow(
        long windowDurationMillis,
        long notFoundToPrefetchDoneMillis,
        long readsOk,
        long readsNotFound,
        long bytesOk,
        long bytesNotFound
    ) {
        if (enabled()) {
            windowDurationHistogram.record(windowDurationMillis);
            if (notFoundToPrefetchDoneMillis >= 0) {
                notFoundToPrefetchDoneHistogram.record(notFoundToPrefetchDoneMillis);
            }
            readsOkHistogram.record(readsOk);
            readsNotFoundHistogram.record(readsNotFound);
            bytesOkHistogram.record(bytesOk);
            bytesNotFoundHistogram.record(bytesNotFound);
        }
    }
}
