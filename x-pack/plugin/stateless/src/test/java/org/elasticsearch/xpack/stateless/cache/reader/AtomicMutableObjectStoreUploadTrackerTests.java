/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.reader.UploadTrackerMetrics.ReadOutcome;
import org.elasticsearch.xpack.stateless.cache.reader.UploadTrackerMetrics.VbccNotFoundCause;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class AtomicMutableObjectStoreUploadTrackerTests extends ESTestCase {

    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private final AtomicBoolean metricsEnabled = new AtomicBoolean(true);
    private final AtomicLong clock = new AtomicLong();
    private final AtomicMutableObjectStoreUploadTracker tracker = new AtomicMutableObjectStoreUploadTracker(
        UploadTrackerMetrics.register(meterRegistry, metricsEnabled::get),
        new ShardId("index", "_na_", 0),
        clock::get
    );

    private static PrimaryTermAndGeneration bcc(long generation) {
        return new PrimaryTermAndGeneration(1L, generation);
    }

    private List<Measurement> counter(String name) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name);
    }

    private List<Measurement> histogram(String name) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, name);
    }

    private List<Measurement> notFoundMeasurements() {
        return counter(UploadTrackerMetrics.VBCC_NOT_FOUND_TOTAL_METRIC);
    }

    private static Map<String, Object> cause(VbccNotFoundCause cause) {
        return Map.of(UploadTrackerMetrics.CAUSE_ATTRIBUTE, cause.attributeValue());
    }

    private void assertNotFoundCauses(VbccNotFoundCause... expectedCauses) {
        var measurements = notFoundMeasurements();
        assertThat(measurements, hasSize(expectedCauses.length));
        for (int i = 0; i < expectedCauses.length; i++) {
            assertThat(measurements.get(i).getLong(), equalTo(1L));
            assertThat(measurements.get(i).attributes(), equalTo(cause(expectedCauses[i])));
        }
    }

    private long single(String histogramName) {
        var measurements = histogram(histogramName);
        assertThat(histogramName, measurements, hasSize(1));
        return measurements.getFirst().getLong();
    }

    private void assertNoWindowSummary() {
        assertThat(histogram(UploadTrackerMetrics.WINDOW_DURATION_HISTOGRAM_METRIC), empty());
        assertThat(histogram(UploadTrackerMetrics.READS_OK_HISTOGRAM_METRIC), empty());
    }

    /// Mimics `SearchEngine#onCommitNotification` at the end of a prefetch: the tracker is updated before the window is closed.
    private void prefetchDone(VbccEpisodeToken token, PrimaryTermAndGeneration latestUploaded) {
        tracker.updateLatestUploadedBcc(latestUploaded);
        token.closeWindow();
    }

    public void testNotificationPendingWhenNoWindowIsOpen() {
        var handle = tracker.registerVbccRead(bcc(5), 10L);
        assertThat(handle.isRegistered(), is(false));
        tracker.onVbccNotFound(bcc(5), handle);
        assertNotFoundCauses(VbccNotFoundCause.NOTIFICATION_PENDING);
    }

    public void testNotificationPendingWhenOnlyAnOlderWindowIsOpen() {
        tracker.openEpisode(bcc(4));
        var handle = tracker.registerVbccRead(bcc(5), 10L);
        assertThat(handle.isRegistered(), is(false));
        tracker.onVbccNotFound(bcc(5), handle);
        assertNotFoundCauses(VbccNotFoundCause.NOTIFICATION_PENDING);
    }

    public void testAlreadyUpdatedWhenTheReadDidNotStartInAWindowAndTheTrackerKnowsTheBccIsUploaded() {
        tracker.updateLatestUploadedBcc(bcc(5));
        tracker.onVbccNotFound(bcc(5), VbccReadHandle.NOOP);
        assertNotFoundCauses(VbccNotFoundCause.ALREADY_UPDATED);
    }

    public void testEveryReadStartedInTheWindowIsPrefetchPending() {
        tracker.openEpisode(bcc(5));
        var first = tracker.registerVbccRead(bcc(5), 10L);
        var second = tracker.registerVbccRead(bcc(4), 10L);
        var third = tracker.registerVbccRead(bcc(5), 10L);
        assertThat(first.isRegistered() && second.isRegistered() && third.isRegistered(), is(true));

        // the first failure updates the tracker, the next ones are the reads that were in flight and are still part of the window
        tracker.onVbccNotFound(bcc(5), first);
        tracker.onVbccNotFound(bcc(4), second);
        tracker.onVbccNotFound(bcc(5), third);
        assertNotFoundCauses(VbccNotFoundCause.PREFETCH_PENDING, VbccNotFoundCause.PREFETCH_PENDING, VbccNotFoundCause.PREFETCH_PENDING);
    }

    public void testReadFailingAfterThePrefetchIsDoneIsStillPrefetchPending() {
        var token = tracker.openEpisode(bcc(5));
        var handle = tracker.registerVbccRead(bcc(5), 10L);
        prefetchDone(token, bcc(5));
        assertNoWindowSummary();

        tracker.onVbccNotFound(bcc(5), handle);
        assertNotFoundCauses(VbccNotFoundCause.PREFETCH_PENDING);
        assertThat(single(UploadTrackerMetrics.READS_NOT_FOUND_HISTOGRAM_METRIC), equalTo(1L));
    }

    public void testWindowReadIsCountedByOutcome() {
        var token = tracker.openEpisode(bcc(5));
        var ok = tracker.registerVbccRead(bcc(5), 100L);
        var notFound = tracker.registerVbccRead(bcc(5), 30L);
        ok.onSuccess();
        tracker.onVbccNotFound(bcc(5), notFound);
        prefetchDone(token, bcc(5));

        var reads = counter(UploadTrackerMetrics.READS_IN_WINDOW_TOTAL_METRIC);
        assertThat(reads, hasSize(2));
        assertThat(reads.get(0).attributes(), equalTo(Map.of(UploadTrackerMetrics.OUTCOME_ATTRIBUTE, ReadOutcome.OK.attributeValue())));
        assertThat(
            reads.get(1).attributes(),
            equalTo(Map.of(UploadTrackerMetrics.OUTCOME_ATTRIBUTE, ReadOutcome.NOT_FOUND.attributeValue()))
        );
        var bytes = counter(UploadTrackerMetrics.READ_BYTES_IN_WINDOW_TOTAL_METRIC);
        assertThat(bytes.get(0).getLong(), equalTo(100L));
        assertThat(bytes.get(1).getLong(), equalTo(30L));
    }

    public void testReadStartedAfterTheWindowIsClosedIsNotRegistered() {
        var token = tracker.openEpisode(bcc(5));
        prefetchDone(token, bcc(5));
        assertThat(tracker.registerVbccRead(bcc(5), 10L).isRegistered(), is(false));
    }

    public void testWindowKeyIsAMaximum() {
        tracker.openEpisode(bcc(5));
        assertThat(tracker.registerVbccRead(bcc(4), 10L).isRegistered(), is(true));
        assertThat(tracker.registerVbccRead(bcc(5), 10L).isRegistered(), is(true));
        assertThat(tracker.registerVbccRead(bcc(6), 10L).isRegistered(), is(false));
    }

    public void testReadIsAttributedToTheSmallestWindowThatCoversIt() {
        var tokenFor5 = tracker.openEpisode(bcc(5));
        var tokenFor8 = tracker.openEpisode(bcc(8));
        var handle = tracker.registerVbccRead(bcc(4), 10L);
        handle.onSuccess();
        prefetchDone(tokenFor8, bcc(8));
        assertNoWindowSummary();
        // the window of 5 is still open and got the read
        tokenFor5.closeWindow();
        assertThat(single(UploadTrackerMetrics.READS_OK_HISTOGRAM_METRIC), equalTo(1L));
    }

    public void testWindowIsOnlyClosedByTheLastPrefetchOfTheSameKey() {
        var first = tracker.openEpisode(bcc(5));
        var second = tracker.openEpisode(bcc(5));

        first.closeWindow();
        // closing a token twice must not close the window while another prefetch is running
        first.closeWindow();
        assertThat(tracker.registerVbccRead(bcc(5), 10L).isRegistered(), is(true));

        second.closeWindow();
        assertThat(tracker.registerVbccRead(bcc(5), 10L).isRegistered(), is(false));
    }

    public void testNewWindowCanBeOpenedForTheSameKeyAfterTheLastOneWasClosed() {
        // the tracker is not updated, as when the prefetch failed
        tracker.openEpisode(bcc(5)).closeWindow();
        var token = tracker.openEpisode(bcc(5));
        assertThat(tracker.registerVbccRead(bcc(5), 10L).isRegistered(), is(true));
        token.closeWindow();
    }

    public void testNoWindowIsOpenedForABccTheTrackerAlreadyKnowsIsUploaded() {
        tracker.updateLatestUploadedBcc(bcc(5));
        tracker.openEpisode(bcc(5));
        tracker.openEpisode(bcc(3));
        tracker.openEpisode(null);
        assertThat(tracker.registerVbccRead(bcc(5), 10L).isRegistered(), is(false));
        assertThat(tracker.registerVbccRead(bcc(2), 10L).isRegistered(), is(false));
    }

    public void testSummaryIsEmittedOnceAfterTheLastReadInFlight() {
        clock.set(100L);
        var token = tracker.openEpisode(bcc(5));
        var failing = tracker.registerVbccRead(bcc(5), 40L);
        var inFlight = tracker.registerVbccRead(bcc(4), 60L);
        var ok = tracker.registerVbccRead(bcc(5), 8L);

        clock.set(250L);
        ok.onSuccess();
        tracker.onVbccNotFound(bcc(5), failing);

        clock.set(400L);
        prefetchDone(token, bcc(5));
        // a read that started inside the window is still running
        assertNoWindowSummary();

        clock.set(450L);
        inFlight.onSuccess();
        // completing a read twice has no effect
        inFlight.onSuccess();
        inFlight.onNotFound();

        assertThat(single(UploadTrackerMetrics.WINDOW_DURATION_HISTOGRAM_METRIC), equalTo(300L));
        assertThat(single(UploadTrackerMetrics.NOT_FOUND_TO_PREFETCH_DONE_HISTOGRAM_METRIC), equalTo(150L));
        assertThat(single(UploadTrackerMetrics.READS_OK_HISTOGRAM_METRIC), equalTo(2L));
        assertThat(single(UploadTrackerMetrics.READS_NOT_FOUND_HISTOGRAM_METRIC), equalTo(1L));
        assertThat(single(UploadTrackerMetrics.BYTES_OK_HISTOGRAM_METRIC), equalTo(68L));
        assertThat(single(UploadTrackerMetrics.BYTES_NOT_FOUND_HISTOGRAM_METRIC), equalTo(40L));
        assertNotFoundCauses(VbccNotFoundCause.PREFETCH_PENDING);
    }

    public void testNotFoundToPrefetchDoneIsZeroWhenTheFirstFailureHappensAfterThePrefetchIsDone() {
        clock.set(100L);
        var token = tracker.openEpisode(bcc(5));
        var handle = tracker.registerVbccRead(bcc(5), 10L);
        clock.set(200L);
        prefetchDone(token, bcc(5));
        clock.set(300L);
        tracker.onVbccNotFound(bcc(5), handle);
        assertThat(single(UploadTrackerMetrics.NOT_FOUND_TO_PREFETCH_DONE_HISTOGRAM_METRIC), equalTo(0L));
    }

    public void testNoNotFoundToPrefetchDoneWhenNoReadFailed() {
        var token = tracker.openEpisode(bcc(5));
        tracker.registerVbccRead(bcc(5), 10L).onSuccess();
        prefetchDone(token, bcc(5));
        assertThat(single(UploadTrackerMetrics.READS_OK_HISTOGRAM_METRIC), equalTo(1L));
        assertThat(histogram(UploadTrackerMetrics.NOT_FOUND_TO_PREFETCH_DONE_HISTOGRAM_METRIC), empty());
    }

    public void testNoSummaryWhenNoReadStartedInTheWindow() {
        var token = tracker.openEpisode(bcc(5));
        prefetchDone(token, bcc(5));
        assertNoWindowSummary();
    }

    public void testNoSummaryWhenTheOnlyReadFailedForAnotherReason() {
        var token = tracker.openEpisode(bcc(5));
        var handle = tracker.registerVbccRead(bcc(5), 10L);
        handle.onOtherFailure();
        prefetchDone(token, bcc(5));
        assertNoWindowSummary();
        assertThat(counter(UploadTrackerMetrics.READS_IN_WINDOW_TOTAL_METRIC), empty());
    }

    public void testNothingIsTrackedWhenMetricsAreDisabledButTrackerIsStillUpdated() {
        metricsEnabled.set(false);
        var token = tracker.openEpisode(bcc(5));
        assertThat(token, is(VbccEpisodeToken.NOOP));
        var handle = tracker.registerVbccRead(bcc(5), 10L);
        assertThat(handle, is(VbccReadHandle.NOOP));

        tracker.onVbccNotFound(bcc(5), handle);
        assertThat(notFoundMeasurements(), empty());
        assertThat(tracker.getLatestUploadInfo(bcc(5)).isUploaded(), is(true));

        metricsEnabled.set(true);
        tracker.onVbccNotFound(bcc(6), VbccReadHandle.NOOP);
        assertNotFoundCauses(VbccNotFoundCause.NOTIFICATION_PENDING);
    }

    public void testNothingIsRecordedWhenMetricsAreDisabledDuringTheWindow() {
        var token = tracker.openEpisode(bcc(5));
        var handle = tracker.registerVbccRead(bcc(5), 10L);
        metricsEnabled.set(false);
        tracker.onVbccNotFound(bcc(5), handle);
        prefetchDone(token, bcc(5));
        assertThat(notFoundMeasurements(), empty());
        assertThat(counter(UploadTrackerMetrics.READS_IN_WINDOW_TOTAL_METRIC), empty());
        assertNoWindowSummary();
    }

    public void testNotificationOfABccDoesNotMakeItUploaded() {
        tracker.openEpisode(bcc(5));
        assertThat(tracker.getLatestUploadInfo(bcc(5)).isUploaded(), is(false));
    }

    public void testVbccNotFoundUpdatesTheTracker() {
        tracker.onVbccNotFound(bcc(5), VbccReadHandle.NOOP);
        assertThat(tracker.getLatestUploadInfo(bcc(5)).isUploaded(), is(true));
        assertThat(tracker.getLatestUploadInfo(bcc(6)).isUploaded(), is(false));

        // a second failure on the same BCC is a read that was in flight, as the first failure already updated the tracker
        tracker.onVbccNotFound(bcc(5), VbccReadHandle.NOOP);
        assertNotFoundCauses(VbccNotFoundCause.NOTIFICATION_PENDING, VbccNotFoundCause.ALREADY_UPDATED);
    }

    public void testConcurrentReadsAndPrefetchesAreAllCounted() throws Exception {
        final int threads = between(2, 8);
        final int readsPerThread = between(50, 200);
        var token = tracker.openEpisode(bcc(5));
        final var barrier = new CyclicBarrier(threads + 1);
        final var workers = new ArrayList<Thread>();
        for (int i = 0; i < threads; i++) {
            final boolean failing = i % 2 == 0;
            workers.add(new Thread(() -> {
                safeAwait(barrier);
                for (int j = 0; j < readsPerThread; j++) {
                    var handle = tracker.registerVbccRead(bcc(5), 1L);
                    if (failing) {
                        tracker.onVbccNotFound(bcc(5), handle);
                    } else {
                        handle.onSuccess();
                    }
                }
            }));
        }
        workers.forEach(Thread::start);
        safeAwait(barrier);
        // the prefetch ends while reads are running: reads registered before are counted in the window, the others are not tracked
        prefetchDone(token, bcc(5));
        for (var worker : workers) {
            worker.join();
        }

        long ok = histogram(UploadTrackerMetrics.READS_OK_HISTOGRAM_METRIC).stream().mapToLong(Measurement::getLong).sum();
        long notFound = histogram(UploadTrackerMetrics.READS_NOT_FOUND_HISTOGRAM_METRIC).stream().mapToLong(Measurement::getLong).sum();
        // every registered read is reported in exactly one summary, and every failed registered read has a pending-prefetch cause
        long pending = notFoundMeasurements().stream()
            .filter(m -> m.attributes().equals(cause(VbccNotFoundCause.PREFETCH_PENDING)))
            .count();
        assertThat(pending, equalTo(notFound));
        assertThat(histogram(UploadTrackerMetrics.WINDOW_DURATION_HISTOGRAM_METRIC).size() <= 1, is(true));
        assertThat(ok + notFound <= (long) threads * readsPerThread, is(true));
    }
}
