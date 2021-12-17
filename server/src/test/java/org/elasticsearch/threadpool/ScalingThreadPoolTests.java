/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionHandler;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedRunnable;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ScalingThreadPoolTests extends ESThreadPoolTestCase {

    public void testScalingThreadPoolConfiguration() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final Settings.Builder builder = Settings.builder();

        final int core;
        if (randomBoolean()) {
            core = randomIntBetween(0, 8);
            builder.put("thread_pool." + threadPoolName + ".core", core);
        } else {
            core = "generic".equals(threadPoolName) ? 4 : 1; // the defaults
        }

        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        final int maxBasedOnNumberOfProcessors;
        final int processors;
        if (randomBoolean()) {
            processors = randomIntBetween(1, availableProcessors);
            maxBasedOnNumberOfProcessors = expectedSize(threadPoolName, processors);
            builder.put("node.processors", processors);
        } else {
            maxBasedOnNumberOfProcessors = expectedSize(threadPoolName, availableProcessors);
            processors = availableProcessors;
        }

        final int expectedMax;
        if (maxBasedOnNumberOfProcessors < core || randomBoolean()) {
            expectedMax = randomIntBetween(Math.max(1, core), 16);
            builder.put("thread_pool." + threadPoolName + ".max", expectedMax);
        } else {
            expectedMax = maxBasedOnNumberOfProcessors;
        }

        final long keepAlive;
        if (randomBoolean()) {
            keepAlive = randomIntBetween(1, 300);
            builder.put("thread_pool." + threadPoolName + ".keep_alive", keepAlive + "s");
        } else {
            keepAlive = "generic".equals(threadPoolName) || ThreadPool.Names.SNAPSHOT_META.equals(threadPoolName) ? 30 : 300; // the
                                                                                                                              // defaults
        }

        runScalingThreadPoolTest(builder.build(), (clusterSettings, threadPool) -> {
            final Executor executor = threadPool.executor(threadPoolName);
            assertThat(executor, instanceOf(EsThreadPoolExecutor.class));
            final EsThreadPoolExecutor esThreadPoolExecutor = (EsThreadPoolExecutor) executor;
            final ThreadPool.Info info = info(threadPool, threadPoolName);

            assertThat(info.getName(), equalTo(threadPoolName));
            assertThat(info.getThreadPoolType(), equalTo(ThreadPool.ThreadPoolType.SCALING));

            assertThat(info.getKeepAlive().seconds(), equalTo(keepAlive));
            assertThat(esThreadPoolExecutor.getKeepAliveTime(TimeUnit.SECONDS), equalTo(keepAlive));

            assertNull(info.getQueueSize());
            assertThat(esThreadPoolExecutor.getQueue().remainingCapacity(), equalTo(Integer.MAX_VALUE));

            assertThat(info.getMin(), equalTo(core));
            assertThat(esThreadPoolExecutor.getCorePoolSize(), equalTo(core));
            assertThat(info.getMax(), equalTo(expectedMax));
            assertThat(esThreadPoolExecutor.getMaximumPoolSize(), equalTo(expectedMax));
        });

    }

    private int expectedSize(final String threadPoolName, final int numberOfProcessors) {
        final Map<String, Function<Integer, Integer>> sizes = new HashMap<>();
        sizes.put(ThreadPool.Names.GENERIC, n -> ThreadPool.boundedBy(4 * n, 128, 512));
        sizes.put(ThreadPool.Names.MANAGEMENT, n -> ThreadPool.boundedBy(n, 1, 5));
        sizes.put(ThreadPool.Names.FLUSH, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.REFRESH, ThreadPool::halfAllocatedProcessorsMaxTen);
        sizes.put(ThreadPool.Names.WARMER, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.SNAPSHOT, ThreadPool::halfAllocatedProcessorsMaxFive);
        sizes.put(ThreadPool.Names.SNAPSHOT_META, n -> Math.min(n * 3, 50));
        sizes.put(ThreadPool.Names.FETCH_SHARD_STARTED, ThreadPool::twiceAllocatedProcessors);
        sizes.put(ThreadPool.Names.FETCH_SHARD_STORE, ThreadPool::twiceAllocatedProcessors);
        return sizes.get(threadPoolName).apply(numberOfProcessors);
    }

    public void testScalingThreadPoolIsBounded() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final int size = randomIntBetween(32, 512);
        final Settings settings = Settings.builder().put("thread_pool." + threadPoolName + ".max", size).build();
        runScalingThreadPoolTest(settings, (clusterSettings, threadPool) -> {
            final CountDownLatch latch = new CountDownLatch(1);
            final int numberOfTasks = 2 * size;
            final CountDownLatch taskLatch = new CountDownLatch(numberOfTasks);
            for (int i = 0; i < numberOfTasks; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.await();
                        taskLatch.countDown();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            final ThreadPoolStats.Stats stats = stats(threadPool, threadPoolName);
            assertThat(stats.getQueue(), equalTo(numberOfTasks - size));
            assertThat(stats.getLargest(), equalTo(size));
            latch.countDown();
            try {
                taskLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testScalingThreadPoolThreadsAreTerminatedAfterKeepAlive() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final int min = "generic".equals(threadPoolName) ? 4 : 1;
        final Settings settings = Settings.builder()
            .put("thread_pool." + threadPoolName + ".max", 128)
            .put("thread_pool." + threadPoolName + ".keep_alive", "1ms")
            .build();
        runScalingThreadPoolTest(settings, ((clusterSettings, threadPool) -> {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch taskLatch = new CountDownLatch(128);
            for (int i = 0; i < 128; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.await();
                        taskLatch.countDown();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            int threads = stats(threadPool, threadPoolName).getThreads();
            assertEquals(128, threads);
            latch.countDown();
            // this while loop is the core of this test; if threads
            // are correctly idled down by the pool, the number of
            // threads in the pool will drop to the min for the pool
            // but if threads are not correctly idled down by the pool,
            // this test will just timeout waiting for them to idle
            // down
            do {
                spinForAtLeastOneMillisecond();
            } while (stats(threadPool, threadPoolName).getThreads() > min);
            try {
                taskLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void testScalingThreadPoolRejectAfterShutdown() throws Exception {
        final boolean rejectAfterShutdown = randomBoolean();
        final int min = randomIntBetween(1, 4);
        final int max = randomIntBetween(min, 16);

        final EsThreadPoolExecutor scalingExecutor = EsExecutors.newScaling(
            getTestName().toLowerCase(Locale.ROOT),
            min,
            max,
            randomLongBetween(0, 100),
            TimeUnit.MILLISECONDS,
            rejectAfterShutdown,
            EsExecutors.daemonThreadFactory(getTestName().toLowerCase(Locale.ROOT)),
            new ThreadContext(Settings.EMPTY)
        );
        try {
            final AtomicInteger executed = new AtomicInteger();
            final AtomicInteger rejected = new AtomicInteger();
            final AtomicInteger failed = new AtomicInteger();

            final CountDownLatch latch = new CountDownLatch(max);
            final CountDownLatch block = new CountDownLatch(1);
            for (int i = 0; i < max; i++) {
                execute(scalingExecutor, () -> {
                    try {
                        latch.countDown();
                        block.await();
                    } catch (InterruptedException e) {
                        fail(e.toString());
                    }
                }, executed, rejected, failed);
            }
            latch.await();

            assertThat(scalingExecutor.getCompletedTaskCount(), equalTo(0L));
            assertThat(scalingExecutor.getActiveCount(), equalTo(max));
            assertThat(scalingExecutor.getQueue().size(), equalTo(0));

            final int queued = randomIntBetween(1, 100);
            for (int i = 0; i < queued; i++) {
                execute(scalingExecutor, () -> {}, executed, rejected, failed);
            }

            assertThat(scalingExecutor.getCompletedTaskCount(), equalTo(0L));
            assertThat(scalingExecutor.getActiveCount(), equalTo(max));
            assertThat(scalingExecutor.getQueue().size(), equalTo(queued));

            scalingExecutor.shutdown();

            final int queuedAfterShutdown = randomIntBetween(1, 100);
            for (int i = 0; i < queuedAfterShutdown; i++) {
                execute(scalingExecutor, () -> {}, executed, rejected, failed);
            }
            assertThat(scalingExecutor.getQueue().size(), rejectAfterShutdown ? equalTo(queued) : equalTo(queued + queuedAfterShutdown));

            block.countDown();

            assertBusy(() -> assertTrue(scalingExecutor.isTerminated()));
            assertThat(scalingExecutor.getActiveCount(), equalTo(0));
            assertThat(scalingExecutor.getQueue().size(), equalTo(0));
            assertThat(
                scalingExecutor.getCompletedTaskCount(),
                rejectAfterShutdown ? equalTo((long) max + queued) : greaterThanOrEqualTo((long) max + queued)
            );
            assertThat(
                ((EsRejectedExecutionHandler) scalingExecutor.getRejectedExecutionHandler()).rejected(),
                rejectAfterShutdown ? equalTo((long) queuedAfterShutdown) : equalTo(0L)
            );

        } finally {
            if (scalingExecutor.isShutdown() == false) {
                ThreadPool.terminate(scalingExecutor, 10, TimeUnit.SECONDS);
            }
        }
    }

    private static void execute(
        final Executor executor,
        final CheckedRunnable<Exception> runnable,
        final AtomicInteger executed,
        final AtomicInteger rejected,
        final AtomicInteger failed
    ) {
        if (randomBoolean()) {
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    runnable.run();
                    executed.incrementAndGet();
                }

                @Override
                public void onFailure(Exception e) {
                    failed.incrementAndGet();
                }

                @Override
                public void onRejection(Exception e) {
                    rejected.incrementAndGet();
                }
            });
        } else {
            try {
                executor.execute(() -> {
                    try {
                        runnable.run();
                        executed.incrementAndGet();
                    } catch (Exception e) {
                        failed.incrementAndGet();
                    }
                });
            } catch (EsRejectedExecutionException e) {
                rejected.incrementAndGet();
            } catch (Exception e) {
                failed.incrementAndGet();
            }
        }
    }

    public void runScalingThreadPoolTest(final Settings settings, final BiConsumer<ClusterSettings, ThreadPool> consumer)
        throws InterruptedException {
        ThreadPool threadPool = null;
        try {
            final String test = Thread.currentThread().getStackTrace()[2].getMethodName();
            final Settings nodeSettings = Settings.builder().put(settings).put("node.name", test).build();
            threadPool = new ThreadPool(nodeSettings);
            final ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            consumer.accept(clusterSettings, threadPool);
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }
}
