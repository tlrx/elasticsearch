/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.reader.UploadTrackerMetrics.VbccNotFoundCause;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class AtomicMutableObjectStoreUploadTrackerTests extends ESTestCase {

    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private final AtomicBoolean metricsEnabled = new AtomicBoolean(true);
    private final AtomicMutableObjectStoreUploadTracker tracker = new AtomicMutableObjectStoreUploadTracker(
        UploadTrackerMetrics.register(meterRegistry, metricsEnabled::get)
    );

    private static PrimaryTermAndGeneration bcc(long generation) {
        return new PrimaryTermAndGeneration(1L, generation);
    }

    private List<Measurement> measurements() {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, UploadTrackerMetrics.VBCC_NOT_FOUND_TOTAL_METRIC);
    }

    private void assertSingleMeasurement(VbccNotFoundCause expectedCause) {
        var measurements = measurements();
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getLong(), equalTo(1L));
        assertThat(
            measurements.getFirst().attributes(),
            equalTo(Map.of(UploadTrackerMetrics.CAUSE_ATTRIBUTE, expectedCause.attributeValue()))
        );
    }

    public void testNotificationPendingWhenNothingWasNotified() {
        tracker.onVbccNotFound(bcc(5));
        assertSingleMeasurement(VbccNotFoundCause.NOTIFICATION_PENDING);
    }

    public void testNotificationPendingWhenOnlyAnOlderBccWasNotified() {
        tracker.updateLatestNotifiedBcc(bcc(4));
        tracker.onVbccNotFound(bcc(5));
        assertSingleMeasurement(VbccNotFoundCause.NOTIFICATION_PENDING);
    }

    public void testPrefetchPendingWhenBccWasNotifiedButTrackerIsNotUpdated() {
        tracker.updateLatestNotifiedBcc(bcc(5));
        tracker.onVbccNotFound(bcc(5));
        assertSingleMeasurement(VbccNotFoundCause.PREFETCH_PENDING);
    }

    public void testPrefetchPendingWhenAMoreRecentBccWasNotified() {
        tracker.updateLatestNotifiedBcc(bcc(7));
        tracker.onVbccNotFound(bcc(5));
        assertSingleMeasurement(VbccNotFoundCause.PREFETCH_PENDING);
    }

    public void testAlreadyUpdatedWhenTrackerKnowsTheBccIsUploaded() {
        tracker.updateLatestNotifiedBcc(bcc(5));
        tracker.updateLatestUploadedBcc(bcc(5));
        tracker.onVbccNotFound(bcc(5));
        assertSingleMeasurement(VbccNotFoundCause.ALREADY_UPDATED);
    }

    public void testNotifiedBccIsMonotonicAndIgnoresNull() {
        tracker.updateLatestNotifiedBcc(bcc(7));
        tracker.updateLatestNotifiedBcc(bcc(3));
        tracker.updateLatestNotifiedBcc(null);
        tracker.onVbccNotFound(bcc(7));
        assertSingleMeasurement(VbccNotFoundCause.PREFETCH_PENDING);
    }

    public void testNotifiedBccDoesNotMakeTheBccUploaded() {
        tracker.updateLatestNotifiedBcc(bcc(5));
        assertThat(tracker.getLatestUploadInfo(bcc(5)).isUploaded(), is(false));
    }

    public void testNothingIsRecordedWhenMetricsAreDisabledButTrackerIsStillUpdated() {
        metricsEnabled.set(false);
        tracker.onVbccNotFound(bcc(5));
        assertThat(measurements(), hasSize(0));
        assertThat(tracker.getLatestUploadInfo(bcc(5)).isUploaded(), is(true));

        metricsEnabled.set(true);
        tracker.onVbccNotFound(bcc(6));
        assertSingleMeasurement(VbccNotFoundCause.NOTIFICATION_PENDING);
    }

    public void testVbccNotFoundUpdatesTheTracker() {
        tracker.onVbccNotFound(bcc(5));
        assertThat(tracker.getLatestUploadInfo(bcc(5)).isUploaded(), is(true));
        assertThat(tracker.getLatestUploadInfo(bcc(6)).isUploaded(), is(false));

        // a second failure on the same BCC is an in-flight read, as the first failure already updated the tracker
        tracker.onVbccNotFound(bcc(5));
        var measurements = measurements();
        assertThat(measurements, hasSize(2));
        assertThat(
            measurements.getLast().attributes(),
            equalTo(Map.of(UploadTrackerMetrics.CAUSE_ATTRIBUTE, VbccNotFoundCause.ALREADY_UPDATED.attributeValue()))
        );
    }
}
