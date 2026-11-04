/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

/**
 * Held by the owner of a prefetch that is announced by a commit notification as targeting an uploaded BCC. Closing the window tells the
 * {@link MutableObjectStoreUploadTracker} that the prefetch is over, i.e. that reads of the BCC do not need to target the VBCC anymore.
 */
public interface VbccEpisodeToken {

    VbccEpisodeToken NOOP = () -> {};

    /**
     * Marks the prefetch tied to this token as done. Idempotent, so that it can safely be called both on the success path and from the
     * final listener that covers every failure path.
     */
    void closeWindow();
}
