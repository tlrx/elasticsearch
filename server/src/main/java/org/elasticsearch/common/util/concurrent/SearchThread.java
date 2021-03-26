/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;

public class SearchThread extends Thread {

    private static final ThreadLocal<Map<String, Object>> MAP_THREAD_LOCAL = ThreadLocal.withInitial(HashMap::new);

    public SearchThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, wrap(target), name, stackSize);
    }

    private static Runnable wrap(Runnable runnable) {
        return () -> {
            try {
                assert assertIsSearchThread();
                runnable.run();
            } finally {
                MAP_THREAD_LOCAL.remove();
            }
        };
    }

    public Map<String, Object> getProfilingMap() {
        final Map<String, Object> current = MAP_THREAD_LOCAL.get();
        assert current != null;
        return current;
    }

    private static boolean assertIsSearchThread() {
        final String threadName = Thread.currentThread().getName();
        assert threadName.contains('[' + ThreadPool.Names.SEARCH + ']')
            // Just in case
            || threadName.startsWith("TEST-")
            || threadName.startsWith("LuceneTestCase") : "current thread [" + threadName + "] may not set thread local search context";
        return true;
    }
}
