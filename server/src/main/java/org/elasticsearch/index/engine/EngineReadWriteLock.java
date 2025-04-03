/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reentrant read/write lock used to guard engine changes in a shard.
 *
 * Implemented as a simple wrapper around a {@link ReentrantReadWriteLock} to make it easier to add/override methods in the future.
 */
public final class EngineReadWriteLock implements ReadWriteLock {

    private final ReentrantReadWriteLock lock;
    private final Lock writeLock;
    private final Lock readLock;

    public EngineReadWriteLock() {
        this(new ReentrantReadWriteLock());
    }

    EngineReadWriteLock(ReentrantReadWriteLock lock) {
        this.lock = lock;
        this.writeLock = this.lock.writeLock();
        //this.readLock = new SkipReentrantReadLockAcquisition(lock.readLock());
        this.readLock = lock.readLock();
    }

    @Override
    public Lock writeLock() {
        return this.writeLock;
    }

    @Override
    public Lock readLock() {
        return this.readLock;
    }

    /**
     * See {@link ReentrantReadWriteLock#isWriteLocked()}
     */
    public boolean isWriteLocked() {
        return lock.isWriteLocked();
    }

    /**
     * See {@link ReentrantReadWriteLock#isWriteLockedByCurrentThread()}
     */
    public boolean isWriteLockedByCurrentThread() {
        return lock.isWriteLockedByCurrentThread();
    }

    /**
     * Returns {@code true} if the number of read locks held by any thread is greater than zero.
     * This method is designed for use in monitoring system state, not for synchronization control.
     *
     * @return {@code true} if any thread holds a read lock and {@code false} otherwise
     */
    public boolean isReadLocked() {
        return lock.getReadLockCount() > 0;
    }

    /**
     * Returns {@code true} if the number of holds on the read lock by the current thread is greater than zero.
     * This method is designed for use in monitoring system state, not for synchronization control.
     *
     * @return {@code true} if the number of holds on the read lock by the current thread is greater than zero, {@code false} otherwise
     */
    public boolean isReadLockedByCurrentThread() {
        return getReadHoldCount() > 0;
    }

    /**
     * See {@link ReentrantReadWriteLock#getReadHoldCount()}
     */
    // package private for tests
    int getReadHoldCount() {
        return lock.getReadHoldCount();
    }

    private static class ReentrantCount {
        int count;
    }

    private class SkipReentrantReadLockAcquisition implements Lock {

        private static final ThreadLocal<ReentrantCount> threadLocalReentrantCounters = ThreadLocal.withInitial(ReentrantCount::new);

        private final ReentrantReadWriteLock.ReadLock readLock;

        private SkipReentrantReadLockAcquisition(ReentrantReadWriteLock.ReadLock readLock) {
            this.readLock = readLock;
        }

        private boolean assertReadHoldCounts() {
            final var readHoldCount = lock.getReadHoldCount();
            assert readHoldCount <= 1 : readHoldCount;
            final var reentrants = threadLocalReentrantCounters.get();
            assert reentrants.count == 0 || readHoldCount == 1;
            return true;
        }

        @Override
        public void lock() {
            assert assertReadHoldCounts();

            if (lock.getReadHoldCount() == 1) {
                // reentrant lock acquisition, increment local counter and skip lock()
                var reentrants = threadLocalReentrantCounters.get();
                reentrants.count += 1;
                return;
            }
            readLock.lock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            // TODO
        }

        @Override
        public boolean tryLock() {
            assert assertReadHoldCounts();

            if (lock.getReadHoldCount() == 1) {
                // reentrant lock acquisition, increment local counter and skip tryLock()
                var reentrants = threadLocalReentrantCounters.get();
                reentrants.count += 1;
                return true;
            }
            return readLock.tryLock();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            // TODO
            return readLock.tryLock(time, unit);
        }

        @Override
        public void unlock() {
            assert assertReadHoldCounts();
            assert lock.getReadHoldCount() > 0;

            // reentrant lock acquisition
            var reentrants = threadLocalReentrantCounters.get();
            reentrants.count -= 1;
            if (0 <= reentrants.count) {
                return;
            }
            threadLocalReentrantCounters.remove();
            readLock.unlock();
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }
}
