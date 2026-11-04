/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.cache.reader.UploadTrackerMetrics.ReadOutcome;
import org.elasticsearch.xpack.stateless.cache.reader.UploadTrackerMetrics.VbccNotFoundCause;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * An atomic reference is used to determine whether a commit has been uploaded to the object store in a thread-safe manner.
 * <p>
 * The tracker also follows the windows (episodes) during which a prefetch is running for a BCC that a commit notification announced as
 * uploaded: the VBCC can already be released by the indexing shard whereas the tracker is only updated once the prefetch is done. Reads of
 * a VBCC that start inside such a window are registered to it, and the window records how many of them succeeded or failed. The tracker
 * itself is not modified by a window, only the reads and the prefetch that complete it are.
 */
public class AtomicMutableObjectStoreUploadTracker implements MutableObjectStoreUploadTracker {

    private static final Logger logger = LogManager.getLogger(AtomicMutableObjectStoreUploadTracker.class);

    // Null means no commit has been uploaded yet.
    private final AtomicReference<PrimaryTermAndGeneration> latestUploaded = new AtomicReference<>(null);

    // Keep the latest non uploaded commit that has been notified (null means no commit has been notified yet)
    private record LatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {}

    private final AtomicReference<LatestCommitInfo> latestCommitInfo = new AtomicReference<>(null);

    // The windows that are currently open, keyed by the latest uploaded BCC announced by the notifications that opened them. The key is a
    // maximum: a notification announcing N as the latest uploaded BCC means that every BCC up to N is uploaded.
    private final ConcurrentSkipListMap<PrimaryTermAndGeneration, Episode> episodes = new ConcurrentSkipListMap<>();

    private final UploadTrackerMetrics metrics;
    private final ShardId shardId;
    private final LongSupplier relativeTimeInMillisSupplier;

    public AtomicMutableObjectStoreUploadTracker() {
        this(UploadTrackerMetrics.NOOP);
    }

    public AtomicMutableObjectStoreUploadTracker(UploadTrackerMetrics metrics) {
        this(metrics, new ShardId("_na_", "_na_", 0), () -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    public AtomicMutableObjectStoreUploadTracker(UploadTrackerMetrics metrics, ShardId shardId, LongSupplier relativeTimeInMillisSupplier) {
        this.metrics = metrics;
        this.shardId = shardId;
        this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
    }

    /**
     * Updates the last uploaded term and generation if the provided term and generation is greater than the current one.
     */
    @Override
    public void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
        latestUploaded.updateAndGet((current) -> {
            if (current == null || (latestUploadedBccTermAndGen != null && latestUploadedBccTermAndGen.compareTo(current) > 0)) {
                return latestUploadedBccTermAndGen;
            }
            return current;
        });
    }

    /**
     * Opens, or joins, the window of the given latest uploaded BCC. Nothing is opened if the metrics are disabled or if the tracker already
     * knows that the BCC is uploaded, as no read can target its VBCC anymore.
     */
    @Override
    public VbccEpisodeToken openEpisode(PrimaryTermAndGeneration latestUploadedBcc) {
        if (latestUploadedBcc == null || metrics.enabled() == false) {
            return VbccEpisodeToken.NOOP;
        }
        final var uploaded = latestUploaded.get();
        if (uploaded != null && latestUploadedBcc.compareTo(uploaded) <= 0) {
            return VbccEpisodeToken.NOOP;
        }
        Episode created = null;
        while (true) {
            final var existing = episodes.get(latestUploadedBcc);
            if (existing != null) {
                if (existing.tryAddPrefetch()) {
                    return existing.newToken();
                }
                // The window just closed, forget it and open a new one.
                episodes.remove(latestUploadedBcc, existing);
                continue;
            }
            if (created == null) {
                created = new Episode(latestUploadedBcc, relativeTimeInMillisSupplier.getAsLong());
            }
            if (episodes.putIfAbsent(latestUploadedBcc, created) == null) {
                return created.newToken();
            }
        }
    }

    /**
     * Registers the read to the window that announced the BCC (or a later one) as uploaded, if any is open. The window with the smallest
     * key that is greater than or equal to the BCC is used: when several windows are open it is an approximation.
     */
    @Override
    public VbccReadHandle registerVbccRead(PrimaryTermAndGeneration bccTermAndGen, long bytes) {
        if (episodes.isEmpty()) {
            return VbccReadHandle.NOOP;
        }
        final var entry = episodes.ceilingEntry(bccTermAndGen);
        if (entry == null) {
            return VbccReadHandle.NOOP;
        }
        final var episode = entry.getValue();
        return episode.tryIncRef() ? new ReadHandle(episode, bytes) : VbccReadHandle.NOOP;
    }

    /**
     * Records the failed read, and then updates the tracker: the cause is determined before the update since the update changes the
     * outcome of the classification. A read that started inside a window is always a pending prefetch, whenever it fails.
     */
    @Override
    public void onVbccNotFound(PrimaryTermAndGeneration bccTermAndGen, VbccReadHandle handle) {
        if (handle.isRegistered()) {
            handle.onNotFound();
        } else {
            metrics.recordVbccNotFound(classifyUnregistered(bccTermAndGen));
        }
        updateLatestUploadedBcc(bccTermAndGen);
    }

    private VbccNotFoundCause classifyUnregistered(PrimaryTermAndGeneration bccTermAndGen) {
        final var uploaded = latestUploaded.get();
        if (uploaded != null && bccTermAndGen.compareTo(uploaded) <= 0) {
            return VbccNotFoundCause.ALREADY_UPDATED;
        }
        return VbccNotFoundCause.NOTIFICATION_PENDING;
    }

    /**
     * Updates the latest commit term and generation information if the provided term and generation is greater than the current one, and
     * also updates the preferred indexing node ID to reach out to read it in case it has not been uploaded yet.
     */
    @Override
    public void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
        latestCommitInfo.updateAndGet((current) -> {
            if (current == null || (ccTermAndGen != null && ccTermAndGen.compareTo(current.ccTermAndGen) > 0)) {
                return new LatestCommitInfo(ccTermAndGen, nodeId);
            }
            return current;
        });
    }

    private String getLatestNodeIdOrNull() {
        var latest = latestCommitInfo.get();
        return latest != null ? latest.nodeId : null;
    }

    @Override
    public UploadInfo getLatestUploadInfo(PrimaryTermAndGeneration bccTermAndGen) {
        // capture the preferred node id at the time the file is opened (can be null if no commit have been notified yet, in which case the
        // search shard should not try to reach the indexing shard)
        var preferredNodeId = getLatestNodeIdOrNull();
        return new UploadInfo() {
            @Override
            public boolean isUploaded() {
                var uploaded = latestUploaded.get();
                return uploaded != null && bccTermAndGen.compareTo(uploaded) <= 0;
            }

            @Override
            public String preferredNodeId() {
                return preferredNodeId;
            }
        };
    }

    /**
     * A window. It is reference counted: one reference is held while the window is open, and one by each read registered to it, so that a
     * summary is emitted once the prefetch is done and the last read registered to the window has completed.
     */
    private final class Episode extends AbstractRefCounted {

        private final PrimaryTermAndGeneration key;
        private final long createdMillis;
        // The number of notifications of this window whose prefetch is not done yet, the window is open while it is positive.
        private final AtomicInteger prefetchesInFlight = new AtomicInteger(1);
        private volatile long closedMillis = -1L;
        private final AtomicLong firstNotFoundMillis = new AtomicLong(-1L);
        private final LongAdder readsOk = new LongAdder();
        private final LongAdder readsNotFound = new LongAdder();
        private final LongAdder bytesOk = new LongAdder();
        private final LongAdder bytesNotFound = new LongAdder();

        private Episode(PrimaryTermAndGeneration key, long createdMillis) {
            this.key = key;
            this.createdMillis = createdMillis;
        }

        /**
         * @return false if the window is already closed
         */
        private boolean tryAddPrefetch() {
            while (true) {
                final int current = prefetchesInFlight.get();
                if (current == 0) {
                    return false;
                }
                if (prefetchesInFlight.compareAndSet(current, current + 1)) {
                    return true;
                }
            }
        }

        private VbccEpisodeToken newToken() {
            final var closed = new AtomicBoolean();
            return () -> {
                if (closed.compareAndSet(false, true)) {
                    prefetchDone();
                }
            };
        }

        private void prefetchDone() {
            if (prefetchesInFlight.decrementAndGet() == 0) {
                // New reads must not be registered anymore, the ones that were already registered keep the window alive.
                episodes.remove(key, this);
                closedMillis = relativeTimeInMillisSupplier.getAsLong();
                decRef();
            }
        }

        @Override
        protected void closeInternal() {
            final long ok = readsOk.sum();
            final long notFound = readsNotFound.sum();
            if (ok + notFound == 0L) {
                // No read started in this window, so there is nothing to report that the prefetch metrics do not already tell.
                return;
            }
            final long windowDurationMillis = closedMillis - createdMillis;
            final long firstNotFound = firstNotFoundMillis.get();
            // A read can fail after the prefetch is done: then the failure did not outlive the prefetch.
            final long notFoundToPrefetchDoneMillis = firstNotFound < 0L ? -1L : Math.max(0L, closedMillis - firstNotFound);
            metrics.recordWindow(windowDurationMillis, notFoundToPrefetchDoneMillis, ok, notFound, bytesOk.sum(), bytesNotFound.sum());
            logger.debug(
                "{} reads of the VBCC of [{}] started while its prefetch was running: window of [{}ms], [{}] reads succeeded "
                    + "([{}] bytes), [{}] reads failed ([{}] bytes), first failure [{}ms] before the end of the prefetch",
                shardId,
                key,
                windowDurationMillis,
                ok,
                bytesOk.sum(),
                notFound,
                bytesNotFound.sum(),
                notFoundToPrefetchDoneMillis
            );
        }
    }

    /**
     * A read registered to a window. It holds a reference to the window until it completes.
     */
    private final class ReadHandle implements VbccReadHandle {

        private final Episode episode;
        private final long bytes;
        private final AtomicBoolean completed = new AtomicBoolean();

        private ReadHandle(Episode episode, long bytes) {
            this.episode = episode;
            this.bytes = bytes;
        }

        @Override
        public boolean isRegistered() {
            return true;
        }

        @Override
        public void onSuccess() {
            if (completed.compareAndSet(false, true)) {
                episode.readsOk.increment();
                episode.bytesOk.add(bytes);
                metrics.recordReadInWindow(ReadOutcome.OK, bytes);
                episode.decRef();
            }
        }

        @Override
        public void onNotFound() {
            if (completed.compareAndSet(false, true)) {
                episode.readsNotFound.increment();
                episode.bytesNotFound.add(bytes);
                episode.firstNotFoundMillis.compareAndSet(-1L, relativeTimeInMillisSupplier.getAsLong());
                metrics.recordVbccNotFound(VbccNotFoundCause.PREFETCH_PENDING);
                metrics.recordReadInWindow(ReadOutcome.NOT_FOUND, bytes);
                episode.decRef();
            }
        }

        @Override
        public void onOtherFailure() {
            if (completed.compareAndSet(false, true)) {
                episode.decRef();
            }
        }
    }
}
