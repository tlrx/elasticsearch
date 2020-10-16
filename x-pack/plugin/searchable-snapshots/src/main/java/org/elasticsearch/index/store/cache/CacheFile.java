/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CacheFile {

    @FunctionalInterface
    public interface EvictionListener {
        void onEviction(CacheFile evictedCacheFile);
    }

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE,
        StandardOpenOption.SPARSE };

    private final AbstractRefCounted refCounter = new AbstractRefCounted("CacheFile") {
        @Override
        protected void closeInternal() {
            CacheFile.this.finishEviction();
        }
    };

    private final ReentrantReadWriteLock.WriteLock evictionLock;
    private final ReentrantReadWriteLock.ReadLock readLock;

    private final SparseFileTracker tracker;
    private final String description;
    private final Path file;

    private final AtomicBoolean synced;
    private final AtomicReference<List<Tuple<Long, Long>>> lastSyncedRanges;

    private volatile Set<EvictionListener> listeners;
    private volatile boolean evicted;

    @Nullable // if evicted, or there are no listeners
    private volatile FileChannel channel;

    public CacheFile(String description, long length, Path file) {
        this.tracker = new SparseFileTracker(file.toString(), length);
        this.description = Objects.requireNonNull(description);
        this.file = Objects.requireNonNull(file);
        this.listeners = new HashSet<>();
        this.evicted = false;

        final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
        this.evictionLock = cacheLock.writeLock();
        this.readLock = cacheLock.readLock();

        this.synced = new AtomicBoolean(false);
        this.lastSyncedRanges = new AtomicReference<>();

        assert invariant();
    }

    public CacheFile(String description, long length, Path file, long[] ranges) {
        this.tracker = new SparseFileTracker(file.toString(), length);
        final List<Tuple<Long, Long>> syncedRanges = new ArrayList<>();
        for (int i = 0; i < ranges.length; i += 2) {
            final Tuple<Long, Long> range = Tuple.tuple(ranges[i], ranges[i + 1]);
            // TODO Lazy way to initialize ranges, do this in SparseFileTracker ctor instead
            List<SparseFileTracker.Gap> gaps = tracker.waitForRange(range, range, ActionListener.wrap(() -> {}));
            assert gaps.size() == 1;
            gaps.forEach(gap -> {
                gap.onProgress(gap.end());
                gap.onCompletion();
            });
            syncedRanges.add(range);
        }
        this.description = Objects.requireNonNull(description);
        this.file = Objects.requireNonNull(file);
        this.listeners = new HashSet<>();
        this.evicted = false;

        final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
        this.evictionLock = cacheLock.writeLock();
        this.readLock = cacheLock.readLock();

        this.synced = new AtomicBoolean(true);
        this.lastSyncedRanges = new AtomicReference<>(syncedRanges);

        assert invariant();
    }

    public long getLength() {
        return tracker.getLength();
    }

    public Path getFile() {
        return file;
    }

    Releasable fileLock() {
        boolean success = false;
        readLock.lock();
        try {
            ensureOpen();
            // check if we have a channel while holding the read lock
            if (channel == null) {
                throw new AlreadyClosedException("Cache file channel has been released and closed");
            }
            success = true;
            return readLock::unlock;
        } finally {
            if (success == false) {
                readLock.unlock();
            }
        }
    }

    @Nullable
    FileChannel getChannel() {
        return channel;
    }

    public boolean acquire(final EvictionListener listener) throws IOException {
        assert listener != null;

        ensureOpen();
        boolean success = false;
        if (refCounter.tryIncRef()) {
            evictionLock.lock();
            try {
                ensureOpen();
                final Set<EvictionListener> newListeners = new HashSet<>(listeners);
                final boolean added = newListeners.add(listener);
                assert added : "listener already exists " + listener;
                maybeOpenFileChannel(newListeners);
                listeners = Collections.unmodifiableSet(newListeners);
                success = true;
            } finally {
                try {
                    if (success == false) {
                        refCounter.decRef();
                    }
                } finally {
                    evictionLock.unlock();
                }
            }
        }
        assert invariant();
        return success;
    }

    public boolean release(final EvictionListener listener) {
        assert listener != null;

        boolean success = false;
        evictionLock.lock();
        try {
            try {
                final Set<EvictionListener> newListeners = new HashSet<>(listeners);
                final boolean removed = newListeners.remove(Objects.requireNonNull(listener));
                assert removed : "listener does not exist " + listener;
                if (removed == false) {
                    throw new IllegalStateException("Cannot remove an unknown listener");
                }
                maybeCloseFileChannel(newListeners);
                listeners = Collections.unmodifiableSet(newListeners);
                success = true;
            } finally {
                if (success) {
                    refCounter.decRef();
                }
            }
        } finally {
            evictionLock.unlock();
        }
        assert invariant();
        return success;
    }

    private void finishEviction() {
        assert evictionLock.isHeldByCurrentThread();
        assert listeners.isEmpty();
        assert channel == null;
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void startEviction() {
        if (evicted == false) {
            final Set<EvictionListener> evictionListeners = new HashSet<>();
            evictionLock.lock();
            try {
                if (evicted == false) {
                    evicted = true;
                    evictionListeners.addAll(listeners);
                    refCounter.decRef();
                }
            } finally {
                evictionLock.unlock();
            }
            evictionListeners.forEach(listener -> listener.onEviction(this));
        }
        assert invariant();
    }

    boolean isSynced() {
        return synced.get();
    }

    public List<Tuple<Long, Long>> getLastSyncedRanges(boolean clear) {
        if (clear) {
            return lastSyncedRanges.getAndSet(null);
        } else {
            return lastSyncedRanges.get();
        }
    }

    public boolean sync() throws IOException {
        ensureOpen();
        boolean success = false;
        if (refCounter.tryIncRef()) {
            evictionLock.lock();
            try {
                ensureOpen();
                if (synced.compareAndSet(false, true)) {
                    if (channel != null) {
                        channel.force(true);
                    } else {
                        IOUtils.fsync(file, false);
                    }
                    lastSyncedRanges.set(tracker.getCompletedRanges()); // TODO set back to null in case of fsync failure
                    success = true;
                }
            } finally {
                try {
                    refCounter.decRef();
                } finally {
                    evictionLock.unlock();
                }
            }
        }
        assert invariant();
        return success;
    }

    private FileChannel openFileChannel() throws IOException {
        assert channel == null;
        return FileChannel.open(file, OPEN_OPTIONS);
    }

    private void maybeOpenFileChannel(Set<EvictionListener> listeners) throws IOException {
        assert evictionLock.isHeldByCurrentThread();
        if (listeners.size() == 1) {
            channel = openFileChannel();
        }
    }

    private void maybeCloseFileChannel(Set<EvictionListener> listeners) {
        assert evictionLock.isHeldByCurrentThread();
        if (listeners.size() == 0) {
            assert channel != null;
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Exception when closing channel", e);
            } finally {
                channel = null;
            }
        }
    }

    private boolean invariant() {
        readLock.lock();
        try {
            assert listeners != null;
            if (listeners.isEmpty()) {
                assert channel == null;
                assert evicted == false || refCounter.refCount() != 0 || Files.notExists(file);
            } else {
                assert channel != null;
                assert refCounter.refCount() > 0;
                assert channel.isOpen();
                assert Files.exists(file);
            }
        } finally {
            readLock.unlock();
        }
        return true;
    }

    @Override
    public String toString() {
        return "CacheFile{"
            + "desc='"
            + description
            + "', file="
            + file
            + ", length="
            + tracker.getLength()
            + ", channel="
            + (channel != null ? "yes" : "no")
            + ", listeners="
            + listeners.size()
            + ", evicted="
            + evicted
            + ", tracker="
            + tracker
            + '}';
    }

    private void ensureOpen() {
        if (evicted) {
            throw new AlreadyClosedException("Cache file is evicted");
        }
    }

    @FunctionalInterface
    interface RangeReader {
        int readFromCache(long position, ByteBuffer buffer) throws IOException;
    }

    @FunctionalInterface
    interface RangeAvailableHandler {
        int onRangeAvailable(RangeReader rangeReader) throws IOException;
    }

    @FunctionalInterface
    interface RangeWriter {
        int writeToCache(long position, ByteBuffer buffer) throws IOException;
    }

    @FunctionalInterface
    interface RangeMissingHandler {
        void fillCacheRange(RangeWriter rangeWriter, long from, long to) throws IOException;
    }

    /**
     * Populates any missing ranges within {@code rangeToWrite} using the {@link RangeMissingHandler}, and notifies the
     * {@link RangeAvailableHandler} when {@code rangeToRead} is available to read from the file. If {@code rangeToRead} is already
     * available then the {@link RangeAvailableHandler} is called synchronously by this method; if not then the given {@link Executor}
     * processes the missing ranges and notifies the {@link RangeAvailableHandler}.
     *
     * @return a future which returns the result of the {@link RangeAvailableHandler} once it has completed.
     */
    CompletableFuture<Integer> populateAndRead(
        final Tuple<Long, Long> rangeToWrite,
        final Tuple<Long, Long> rangeToRead,
        final RangeAvailableHandler reader,
        final RangeMissingHandler writer,
        final Executor executor
    ) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            ensureOpen();
            final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(rangeToWrite, rangeToRead, ActionListener.wrap(success -> {
                final int read = reader.onRangeAvailable(this::readCacheFile);
                assert read == rangeToRead.v2() - rangeToRead.v1() : "partial read ["
                    + read
                    + "] does not match the range to read ["
                    + rangeToRead.v2()
                    + '-'
                    + rangeToRead.v1()
                    + ']';
                future.complete(read);
            }, future::completeExceptionally));

            if (gaps.isEmpty() == false) {
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(ActionListener.wrap(() -> {}), gaps.size());
                for (SparseFileTracker.Gap gap : gaps) {
                    executor.execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() {
                            try {
                                ensureOpen();
                                if (readLock.tryLock() == false) {
                                    throw new AlreadyClosedException("Cache file channel is being evicted, writing attempt cancelled");
                                }
                                try {
                                    ensureOpen();
                                    if (channel == null) {
                                        throw new AlreadyClosedException("Cache file channel has been released and closed");
                                    }
                                    final RangeWriter rangeWriter = (position, buffer) -> {
                                        final int written = positionalWrite(position, buffer);
                                        gap.onProgress(position + written);
                                        return written;
                                    };
                                    writer.fillCacheRange(rangeWriter, gap.start(), gap.end());
                                    gap.onCompletion();
                                } finally {
                                    readLock.unlock();
                                }
                            } catch (Exception e) {
                                gap.onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            gaps.forEach(gap -> gap.onFailure(e));
                        }

                        @Override
                        public void onAfter() {
                            listener.onResponse(null);
                        }
                    });
                }
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Notifies the {@link RangeAvailableHandler} when {@code rangeToRead} is available to read from the file. If {@code rangeToRead} is
     * already available then the {@link RangeAvailableHandler} is called synchronously by this method; if not, but it is pending, then the
     * {@link RangeAvailableHandler} is notified when the pending ranges have completed. If it contains gaps that are not currently pending
     * then no listeners are registered and this method returns {@code null}.
     *
     * @return a future which returns the result of the {@link RangeAvailableHandler} once it has completed, or {@code null} if the
     *         target range is neither available nor pending.
     */
    @Nullable
    CompletableFuture<Integer> readIfAvailableOrPending(final Tuple<Long, Long> rangeToRead, final RangeAvailableHandler reader) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            ensureOpen();
            if (tracker.waitForRangeIfPending(rangeToRead, ActionListener.wrap(success -> {
                final int read = reader.onRangeAvailable(this::readCacheFile);
                assert read == rangeToRead.v2() - rangeToRead.v1() : "partial read ["
                    + read
                    + "] does not match the range to read ["
                    + rangeToRead.v2()
                    + '-'
                    + rangeToRead.v1()
                    + ']';
                future.complete(read);
            }, future::completeExceptionally))) {
                return future;
            } else {
                return null;
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
            return future;
        }
    }

    public Tuple<Long, Long> getAbsentRangeWithin(long start, long end) {
        ensureOpen();
        return tracker.getAbsentRangeWithin(start, end);
    }

    @SuppressForbidden(reason = "Use positional writes on purpose")
    private int positionalWrite(long start, ByteBuffer byteBuffer) throws IOException {
        assert assertFileChannelOpen(channel);
        final int written = channel.write(byteBuffer, start);
        if (written > 0) {
            synced.set(false);
        }
        return written;
    }

    private int readCacheFile(final long position, final ByteBuffer buffer) throws IOException {
        assert assertFileChannelOpen(channel);
        final int bytesRead = Channels.readFromFileChannel(channel, position, buffer);
        if (bytesRead == -1) {
            throw new EOFException(
                String.format(Locale.ROOT, "unexpected EOF reading [%d-%d] from %s", position, position + buffer.remaining(), this)
            );
        }
        return bytesRead;
    }

    private static boolean assertFileChannelOpen(FileChannel fileChannel) {
        assert fileChannel != null;
        assert fileChannel.isOpen();
        return true;
    }
}
