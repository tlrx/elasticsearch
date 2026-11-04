/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SwitchingCacheBlobReaderTests extends ESTestCase {

    private static final PrimaryTermAndGeneration BCC = new PrimaryTermAndGeneration(1L, 5L);

    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private final AtomicMutableObjectStoreUploadTracker tracker = new AtomicMutableObjectStoreUploadTracker(
        UploadTrackerMetrics.register(meterRegistry, () -> true),
        new ShardId("index", "_na_", 0),
        () -> 0L
    );

    /// A reader that keeps the listener of the read it is asked, so that the test decides when the read completes.
    private static class PendingReader implements CacheBlobReader {
        final AtomicReference<ActionListener<InputStream>> pending = new AtomicReference<>();

        @Override
        public ByteRange getRange(long position, int length, long remainingFileLength) {
            return ByteRange.of(position, position + length);
        }

        @Override
        public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
            assertTrue(pending.compareAndSet(null, listener));
        }
    }

    private static class FailingReader implements CacheBlobReader {
        @Override
        public ByteRange getRange(long position, int length, long remainingFileLength) {
            throw new AssertionError("unexpected");
        }

        @Override
        public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
            throw new AssertionError("must not read from the object store");
        }
    }

    private List<Measurement> counter(String name) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name);
    }

    private List<Measurement> histogram(String name) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, name);
    }

    public void testReadThatStartsInTheWindowAndSucceedsAfterThePrefetchIsCountedInTheWindow() {
        final var vbccReader = new PendingReader();
        final var reader = new SwitchingCacheBlobReader(tracker, BCC, new FailingReader(), vbccReader);
        final var token = tracker.openEpisode(BCC);

        final var future = new PlainActionFuture<InputStream>();
        reader.getRangeInputStream(0L, 1024, future);

        // the prefetch ends while the read is running: the tracker is updated and the window closed
        tracker.updateLatestUploadedBcc(BCC);
        token.closeWindow();
        assertThat(histogram(UploadTrackerMetrics.READS_OK_HISTOGRAM_METRIC), empty());

        vbccReader.pending.get().onResponse(new ByteArrayInputStream(new byte[0]));
        assertThat(future.isDone(), is(true));
        assertThat(histogram(UploadTrackerMetrics.READS_OK_HISTOGRAM_METRIC).getFirst().getLong(), equalTo(1L));
        assertThat(histogram(UploadTrackerMetrics.BYTES_OK_HISTOGRAM_METRIC).getFirst().getLong(), equalTo(1024L));
        assertThat(histogram(UploadTrackerMetrics.READS_NOT_FOUND_HISTOGRAM_METRIC).getFirst().getLong(), equalTo(0L));
    }

    public void testReadThatStartsInTheWindowAndFailsAfterThePrefetchIsPrefetchPending() {
        final var vbccReader = new PendingReader();
        final var reader = new SwitchingCacheBlobReader(tracker, BCC, new FailingReader(), vbccReader);
        final var token = tracker.openEpisode(BCC);

        final var future = new PlainActionFuture<InputStream>();
        reader.getRangeInputStream(0L, 2048, future);

        tracker.updateLatestUploadedBcc(BCC);
        token.closeWindow();

        vbccReader.pending.get().onFailure(new ResourceNotFoundException("VBCC not found"));
        final var exception = expectThrows(Exception.class, future::actionGet);
        assertThat(exception, instanceOf(ResourceAlreadyUploadedException.class));

        final var notFound = counter(UploadTrackerMetrics.VBCC_NOT_FOUND_TOTAL_METRIC);
        assertThat(notFound, hasSize(1));
        assertThat(
            notFound.getFirst().attributes(),
            equalTo(Map.of(UploadTrackerMetrics.CAUSE_ATTRIBUTE, UploadTrackerMetrics.VbccNotFoundCause.PREFETCH_PENDING.attributeValue()))
        );
        assertThat(histogram(UploadTrackerMetrics.READS_NOT_FOUND_HISTOGRAM_METRIC).getFirst().getLong(), equalTo(1L));
        assertThat(histogram(UploadTrackerMetrics.BYTES_NOT_FOUND_HISTOGRAM_METRIC).getFirst().getLong(), equalTo(2048L));
    }

    public void testReadThatStartsBeforeAnyNotificationIsNotCountedInAWindow() {
        final var vbccReader = new PendingReader();
        final var reader = new SwitchingCacheBlobReader(tracker, BCC, new FailingReader(), vbccReader);

        final var future = new PlainActionFuture<InputStream>();
        reader.getRangeInputStream(0L, 1024, future);
        vbccReader.pending.get().onFailure(new ResourceNotFoundException("VBCC not found"));

        expectThrows(Exception.class, future::actionGet);
        final var notFound = counter(UploadTrackerMetrics.VBCC_NOT_FOUND_TOTAL_METRIC);
        assertThat(notFound, hasSize(1));
        assertThat(
            notFound.getFirst().attributes(),
            equalTo(
                Map.of(UploadTrackerMetrics.CAUSE_ATTRIBUTE, UploadTrackerMetrics.VbccNotFoundCause.NOTIFICATION_PENDING.attributeValue())
            )
        );
        assertThat(counter(UploadTrackerMetrics.READS_IN_WINDOW_TOTAL_METRIC), empty());
        // the tracker was updated, so the next reads go to the object store
        assertThat(tracker.getLatestUploadInfo(BCC).isUploaded(), is(true));
    }

    public void testReadThatFailsForAnotherReasonReleasesTheWindow() {
        final var vbccReader = new PendingReader();
        final var reader = new SwitchingCacheBlobReader(tracker, BCC, new FailingReader(), vbccReader);
        final var token = tracker.openEpisode(BCC);

        final var future = new PlainActionFuture<InputStream>();
        reader.getRangeInputStream(0L, 1024, future);
        final var failure = new IllegalStateException("boom");
        vbccReader.pending.get().onFailure(failure);
        assertThat(expectThrows(IllegalStateException.class, future::actionGet), is(failure));

        tracker.updateLatestUploadedBcc(BCC);
        token.closeWindow();
        // the only read failed for another reason, which is not a read of the window
        assertThat(histogram(UploadTrackerMetrics.WINDOW_DURATION_HISTOGRAM_METRIC), empty());
        assertThat(counter(UploadTrackerMetrics.VBCC_NOT_FOUND_TOTAL_METRIC), empty());
    }

    public void testReadOfAnUploadedBccIsNotRegistered() {
        tracker.updateLatestUploadedBcc(BCC);
        final var objectStoreReader = new PendingReader();
        final var reader = new SwitchingCacheBlobReader(tracker, BCC, objectStoreReader, new FailingReader());
        tracker.openEpisode(new PrimaryTermAndGeneration(1L, 6L));

        final var future = new PlainActionFuture<InputStream>();
        reader.getRangeInputStream(0L, 1024, future);
        objectStoreReader.pending.get().onResponse(new ByteArrayInputStream(new byte[0]));

        assertThat(future.isDone(), is(true));
        assertThat(counter(UploadTrackerMetrics.READS_IN_WINDOW_TOTAL_METRIC), empty());
    }
}
