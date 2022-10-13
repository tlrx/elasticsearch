/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.snapshots.blobstore;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Rate limiting wrapper for InputStream
 */
public class RateLimitingInputStream extends FilterInputStream {

    private final Supplier<RateLimiter> rateLimiterSupplier;

    private final Listener listener;

    private long bytesSinceLastRateLimit;

    final AtomicBoolean registerd = new AtomicBoolean();

    public interface Listener {
        void onPause(long nanos);
        default void onBytes(int bytes) {
            ;
        }
    }

    public RateLimitingInputStream(InputStream delegate, Supplier<RateLimiter> rateLimiterSupplier, Listener listener) {
        super(delegate);
        this.rateLimiterSupplier = rateLimiterSupplier;
        this.listener = listener;
    }

    private void maybePause(int bytes) throws IOException {
        bytesSinceLastRateLimit += bytes;
        final RateLimiter rateLimiter = rateLimiterSupplier.get();
        if (rateLimiter != null) {
            if (registerd.compareAndSet(false, true)) {
                rateLimiter.register(this);
            }
            if (bytesSinceLastRateLimit >= rateLimiter.getMinPauseCheckBytes()) {
                long pause = rateLimiter.pause(bytesSinceLastRateLimit, this);
                bytesSinceLastRateLimit = 0;
                if (pause > 0) {
                    listener.onPause(pause);
                }
            }
        }
        listener.onBytes(bytes);
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        maybePause(1);
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len);
        if (n > 0) {
            maybePause(n);
        }
        return n;
    }

    @Override
    public void close() throws IOException {
        try  {
            super.close();
        } finally {
            if (registerd.compareAndSet(true, false)) {
                final RateLimiter rateLimiter = rateLimiterSupplier.get();
                if (rateLimiter != null) {
                    rateLimiter.unregister(this);
                }
            }
        }
    }
}
