/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.support.AdapterActionFuture;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class ProgressListenableActionFuture<T> extends AdapterActionFuture<T, T> implements ListenableActionFuture<T> {

    private final TreeSet<Tuple<Long, ActionListener<T>>> listeners = new TreeSet<>(Comparator.comparingLong(Tuple::v1));
    private final long begin;
    private final long end;

    // modified under 'this' mutex
    private boolean executedListeners;
    private long current;

    private ProgressListenableActionFuture(final long begin, final long end) {
        super();
        assert begin <= end; // TODO
        if (end < begin) {
            // TODO
        }
        this.begin = begin;
        this.end = end;
        this.executedListeners = false;
        this.current = this.begin;
    }

    public static <T> ProgressListenableActionFuture<T> create(final long begin, final long end) {
        return new ProgressListenableActionFuture<>(begin, end);
    }

    public void onProgress(long value) {
        assert executedListeners == false;
        assert begin < value;
        assert value <= end; // TODO

        final List<ActionListener<T>> listeners = new ArrayList<>();
        synchronized (this) {
            this.current = value;
            final Iterator<Tuple<Long, ActionListener<T>>> iterator = this.listeners.iterator();
            while (iterator.hasNext()) {
                final Tuple<Long, ActionListener<T>> tuple = iterator.next();
                if (tuple.v1() <= this.current) {
                    listeners.add(tuple.v2());
                    iterator.remove();
                    System.out.println(">>> releasing at " + tuple.v1() + " / " + this.end);
                }
            }
            assert this.listeners.stream().allMatch(listener -> this.current < listener.v1());
        }
        for (ActionListener<T> listener : listeners) {
            executeListener(listener);
        }
    }

    @Override
    protected void done() {
        super.done();
        synchronized (this) {
            executedListeners = true;
        }
        listeners.stream().map(Tuple::v2).forEach(this::executeListener);
    }

    @Override
    public void addListener(final ActionListener<T> listener) {
        addListener(end, listener);
    }

    public void addListener(long position, ActionListener<T> listener) {
        boolean executeImmediate = false;
        synchronized (this) {
            if (executedListeners) {
                executeImmediate = true;
            } else if (position <= current) {
                executeImmediate = true;
            } else {
                this.listeners.add(Tuple.tuple(position, listener));
            }
        }
        if (executeImmediate) {
            executeListener(listener);
        }
    }

    private void executeListener(final ActionListener<T> listener) {
        try {
            // we use a timeout of 0 to by pass assertion forbidding to call actionGet() (blocking) on a network thread.
            // here we know we will never block
            //listener.onResponse(actionGet(0));
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected T convert(T listenerResponse) {
        return listenerResponse;
    }
}
