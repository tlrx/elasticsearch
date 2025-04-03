/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.hamcrest.Matchers.equalTo;

public class EngineReadWriteLockTests extends ESTestCase {

    public void testReadLock() {
        final var engineLock = new EngineReadWriteLock();
        final var readLock = engineLock.readLock();

        randomLock(readLock);
        try {
            assertThat(engineLock.isReadLockedByCurrentThread(), equalTo(true));
            assertThat(engineLock.getReadHoldCount(), equalTo(1));

            int iters = randomIntBetween(1, 3);
            for (int i = 0; i < iters; i++) {
                recursiveReentrantLocking(engineLock, 1);
            }
        } finally {
            engineLock.readLock().unlock();
        }
        assertThat(engineLock.isReadLockedByCurrentThread(), equalTo(false));
        assertThat(engineLock.getReadHoldCount(), equalTo(0));
    }

    private static void recursiveReentrantLocking(EngineReadWriteLock engineLock, int maxDepth) {
        assertThat(engineLock.getReadHoldCount(), equalTo(1));

        var readLock = engineLock.readLock();
        randomLock(readLock);
        try {
            assertThat(engineLock.getReadHoldCount(), equalTo(1));
            if (randomBoolean() && maxDepth < 10) {
                recursiveReentrantLocking(engineLock, maxDepth + 1);
            }
        } finally {
            engineLock.readLock().unlock();
            assertThat(engineLock.getReadHoldCount(), equalTo(1));
        }
    }

    private static void randomLock(Lock lock) {
        try {
            boolean result = false;
            switch (randomFrom(0, 2)) {
                case 0:
                    lock.lock();
                    result = true;
                    break;
                case 1:
                    lock.lockInterruptibly();
                    result = true;
                    break;
                case 2:
                    result = lock.tryLock();
                    break;
                case 3:
                    result = lock.tryLock(randomLongBetween(0L, 10L), TimeUnit.MILLISECONDS);
                    break;
                default:
                    throw new AssertionError("unknown lock method");
            }
            assertThat(result, equalTo(true));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testReentrancy() throws Exception {
        final var reentrantReadWriteLock = new TestReentrantReadWriteLock();
        final var engineLock = new EngineReadWriteLock(reentrantReadWriteLock);

        var firstReaderAcquired = new CountDownLatch(1);
        var firstReaderAcquiredReentrant = new CountDownLatch(1);
        var secondReaderAcquired = new CountDownLatch(1);
        var secondReaderAcquiredReentrant = new CountDownLatch(1);
        var thirdReaderAcquired = new CountDownLatch(1);

        final var firstWriterStarted = new CountDownLatch(1);
        final var firstWriterAcquired = new CountDownLatch(1);

        final var firstReader = new Thread(() -> {
            engineLock.readLock().lock();
            try {
                firstReaderAcquired.countDown();
                safeAwait(secondReaderAcquired);
                safeAwait(thirdReaderAcquired);

                logger.info("first reader waits for writer to be queued...");
                safeAwait(firstWriterStarted);
                assertBusy(() -> assertTrue(reentrantReadWriteLock.hasQueuedWriterThreads()));

                safeAwait(secondReaderAcquiredReentrant);
                logger.info("first reader reentrant locking...");
                engineLock.readLock().lock();

                logger.info("first reader reentrant locking acquired");
                firstReaderAcquiredReentrant.countDown();

                engineLock.readLock().unlock();
                logger.info("first reader reentrant released");

                engineLock.readLock().unlock();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        final var secondReader = new Thread(() -> {
            engineLock.readLock().lock();
            try {
                secondReaderAcquired.countDown();

                logger.info("second reader waits for third thread to precede...");
                safeAwait(thirdReaderAcquired);

                logger.info("second reader waits for writer to be queued...");
                safeAwait(firstWriterStarted);
                assertBusy(() -> assertTrue(reentrantReadWriteLock.hasQueuedWriterThreads()));

                logger.info("second reader reentrant locking...");
                engineLock.readLock().lock();

                logger.info("second reader reentrant locking acquired");
                secondReaderAcquiredReentrant.countDown();

                engineLock.readLock().unlock();
                engineLock.readLock().unlock();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        final var thirdReader = new Thread(() -> {
            engineLock.readLock().lock();
            try {
                thirdReaderAcquired.countDown();
                safeAwait(secondReaderAcquiredReentrant);
                engineLock.readLock().unlock();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        final var firstWriter = new Thread(() -> {
            safeAwait(firstReaderAcquired);
            safeAwait(secondReaderAcquired);
            safeAwait(thirdReaderAcquired);

            firstWriterStarted.countDown();
            engineLock.writeLock().lock();
            firstWriterAcquired.countDown();
            engineLock.writeLock().unlock();
        });

        firstReader.start();
        secondReader.start();
        thirdReader.start();
        firstWriter.start();

        firstReader.join();
        secondReader.join();
        thirdReader.join();
        firstWriter.join();
    }

    private static class TestReentrantReadWriteLock extends ReentrantReadWriteLock {
        private boolean hasQueuedWriterThreads() {
            return super.getQueuedWriterThreads().isEmpty() == false;
        }
    }
}
