/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

/**
 * Extends {@link ObjectStoreUploadTracker} to allow updating the last uploaded term and generation.
 */
public interface MutableObjectStoreUploadTracker extends ObjectStoreUploadTracker {

    UploadInfo UPLOADED = new UploadInfo() {

        @Override
        public boolean isUploaded() {
            return true;
        }

        @Override
        public String preferredNodeId() {
            assert false : "method should not be called";
            return null;
        }
    };

    MutableObjectStoreUploadTracker ALWAYS_UPLOADED = new MutableObjectStoreUploadTracker() {

        @Override
        public UploadInfo getLatestUploadInfo(PrimaryTermAndGeneration bccTermAndGen) {
            return UPLOADED;
        }

        @Override
        public void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
            // no op
        }

        @Override
        public void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
            // no op
        }
    };

    /**
     * Updates the latest uploaded {@link BatchedCompoundCommit} information.
     * The implementation should ignore the argument if the latest information is already more recent.
     */
    void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen);

    /**
     * Updates the latest {@link StatelessCompoundCommit} information, and the preferred indexing
     * node that can be reached out to read it in case it is not uploaded yet to the object store.
     * The implementation should ignore the arguments if the latest information is already more recent.
     */
    void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId);

    /**
     * Records that a commit notification announced the given {@link BatchedCompoundCommit} as uploaded. This must be called when the
     * notification is received, before its prefetch starts, whereas {@link #updateLatestUploadedBcc} is only called once the prefetch is
     * done. It allows to tell apart a read failing because the prefetch is late from one failing because nothing announced the upload.
     * The implementation should ignore the argument if it is null or if the latest information is already more recent.
     */
    default void updateLatestNotifiedBcc(PrimaryTermAndGeneration notifiedUploadedBccTermAndGen) {}

    /**
     * Called when a read of the given {@link BatchedCompoundCommit} from its VBCC failed because the VBCC was released. The implementation
     * records why the read was still targeting the VBCC and then updates the latest uploaded information like
     * {@link #updateLatestUploadedBcc} so that next reads go to the object store.
     */
    default void onVbccNotFound(PrimaryTermAndGeneration bccTermAndGen) {
        updateLatestUploadedBcc(bccTermAndGen);
    }
}
