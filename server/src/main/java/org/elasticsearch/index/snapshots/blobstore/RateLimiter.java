/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Abstract base class to rate limit IO. Typically implementations are shared across multiple
 * IndexInputs or IndexOutputs (for example those involved all merging). Those IndexInputs and
 * IndexOutputs would call {@link #pause} whenever the have read or written more than {@link
 * #getMinPauseCheckBytes} bytes.
 */
public abstract class RateLimiter {


  /** The current MB per second rate limit. */
  public abstract double getMBPerSec();

  /**
   * Pauses, if necessary, to keep the instantaneous IO rate at or below the target.
   *
   * <p>Note: the implementation is thread-safe
   *
   * @return the pause time in nano seconds
   */
  public abstract long pause(long bytes) throws IOException;

  public long pause(long bytes, Object o) throws IOException {
    throw new UnsupportedEncodingException();
  }

  /**
   * How many bytes caller should add up itself before invoking {@link #pause}. NOTE: The value
   * returned by this method may change over time and is not guaranteed to be constant throughout
   * the lifetime of the RateLimiter. Users are advised to refresh their local values with calls to
   * this method to ensure consistency.
   */
  public abstract long getMinPauseCheckBytes();

    public void register(Object o) {

    }

    public void unregister(Object o) {

    }

    /**
     * Sets an updated MB per second rate limit. A subclass is allowed to perform dynamic updates of
     * the rate limit during use.
     */
    public abstract void setMBPerSec(double mbPerSec);

  /** Simple class to rate limit IO. */
  public static class SimpleRateLimiter extends RateLimiter {

    private static final int MIN_PAUSE_CHECK_MSEC = 5;

    private volatile double mbPerSec;
    private volatile long minPauseCheckBytes;
    private long lastNS;

    /** mbPerSec is the MB/sec max IO rate */
    public SimpleRateLimiter(double mbPerSec) {
      setMBPerSec(mbPerSec);
      lastNS = System.nanoTime();
    }

    /** Sets an updated mb per second rate limit. */
    @Override
    public void setMBPerSec(double mbPerSec) {
      this.mbPerSec = mbPerSec;
      minPauseCheckBytes = (long) ((MIN_PAUSE_CHECK_MSEC / 1000.0) * mbPerSec * 1024 * 1024);
    }

    @Override
    public long getMinPauseCheckBytes() {
      return minPauseCheckBytes;
    }

    /** The current mb per second rate limit. */
    @Override
    public double getMBPerSec() {
      return this.mbPerSec;
    }

  private Map<Object, Tuple<Double, Long>> participants = new IdentityHashMap<>();


  public void register(Object o) {
      synchronized (this) {
          if (participants.containsKey(o)) {
              return;
          }
          int nb = participants.size() + 1;
          Map<Object, Tuple<Double, Long>> map = new IdentityHashMap<>(nb);
          participants.forEach((k,  v) -> map.put(k, Tuple.tuple(mbPerSec / nb, v.v2())));
          map.put(o, Tuple.tuple(mbPerSec / nb, System.nanoTime()));
          participants = map;
          System.out.println("participants+ " + participants.size());
      }
  }

  public void unregister(Object o) {
      synchronized (this) {
          if (participants.remove(o) != null) {
              int nb = participants.size();
              Map<Object, Tuple<Double, Long>> map = new IdentityHashMap<>(nb);
              participants.forEach((k, v) -> {
                  map.put(k, Tuple.tuple(mbPerSec / nb, v.v2()));
              });
              participants = map;
              System.out.println("participants- " + participants.size());
          }
      }
  }

      /**
     * Pauses, if necessary, to keep the instantaneous IO rate at or below the target. Be sure to
     * only call this method when bytes &gt; {@link #getMinPauseCheckBytes}, otherwise it will pause
     * way too long!
     *
     * @return the pause time in nano seconds
     */
    public long pause(long bytes) {
        throw new RuntimeException();
    }

    public long pause(long bytes, Object o) {

      long startNS = System.nanoTime();


      long targetNS;

      // Sync'd to read + write lastNS:
      synchronized (this) {
          Tuple<Double, Long> p = participants.get(o);
          double secondsToPause = (bytes / 1024. / 1024.) / p.v1();

          // Time we should sleep until; this is purely instantaneous
        // rate (just adds seconds onto the last time we had paused to);
        // maybe we should also offer decayed recent history one?
        targetNS = p.v2() + (long) (1000000000 * secondsToPause);

        if (startNS >= targetNS) {
          // OK, current time is already beyond the target sleep time,
          // no pausing to do.

          // Set to startNS, not targetNS, to enforce the instant rate, not
          // the "averaaged over all history" rate:
            participants.compute(o, (o1, tuple) -> Tuple.tuple(tuple.v1(), startNS));
          //lastNS = startNS;
          return 0;
        }

        //lastNS = targetNS;
          participants.compute(o, (o1, tuple) -> Tuple.tuple(tuple.v1(), targetNS));
      }

      long curNS = startNS;

      // While loop because Thread.sleep doesn't always sleep
      // enough:
      while (true) {
        final long pauseNS = targetNS - curNS;
        if (pauseNS > 0) {
          try {
            // NOTE: except maybe on real-time JVMs, minimum realistic sleep time
            // is 1 msec; if you pass just 1 nsec the default impl rounds
            // this up to 1 msec:
            int sleepNS;
            int sleepMS;
            if (pauseNS > 100000L * Integer.MAX_VALUE) {
              // Not really practical (sleeping for 25 days) but we shouldn't overflow int:
              sleepMS = Integer.MAX_VALUE;
              sleepNS = 0;
            } else {
              sleepMS = (int) (pauseNS / 1000000);
              sleepNS = (int) (pauseNS % 1000000);
            }
            Thread.sleep(sleepMS, sleepNS);
          } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
          }
          curNS = System.nanoTime();
          continue;
        }
        break;
      }

      return curNS - startNS;
    }
  }
}
