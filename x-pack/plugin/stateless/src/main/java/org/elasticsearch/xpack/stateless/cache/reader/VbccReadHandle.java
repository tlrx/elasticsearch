/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

/**
 * Represents a read of a VBCC that started while a commit notification already announced the BCC as uploaded but its prefetch was not
 * done yet. The read is attributed to that window even if it completes after the prefetch. Exactly one of the {@code on*} methods must be
 * called when the read completes; further calls are ignored.
 */
public interface VbccReadHandle {

    VbccReadHandle NOOP = new VbccReadHandle() {
        @Override
        public boolean isRegistered() {
            return false;
        }

        @Override
        public void onSuccess() {}

        @Override
        public void onNotFound() {}

        @Override
        public void onOtherFailure() {}
    };

    /**
     * @return whether the read started inside a window. Reads that did not are not tracked: the handle does nothing.
     */
    boolean isRegistered();

    /**
     * The read succeeded.
     */
    void onSuccess();

    /**
     * The read failed because the VBCC was released by the indexing shard.
     */
    void onNotFound();

    /**
     * The read failed for another reason.
     */
    void onOtherFailure();
}
