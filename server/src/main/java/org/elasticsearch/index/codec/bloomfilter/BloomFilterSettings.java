/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BloomFilterSettings {
    private static final int DEFAULT_BLOOM_FILTER_SIZE = Math.toIntExact(ByteSizeValue.ofKb(128).getBytes());
    public static final BloomFilterSettings DEFAULT_BLOOM_FILTER_SETTINGS = new BloomFilterSettings(Settings.EMPTY);

    // Poor man's settings
    public static final AtomicBoolean SKIP_LOOKUP = new AtomicBoolean(false);
    public static final AtomicBoolean FORCE_LOOKUP = new AtomicBoolean(false);
    public static final AtomicBoolean LOAD_BLOOM_FILTER_IN_MEMORY = new AtomicBoolean(true);
    public static final AtomicInteger BLOOM_FILTER_SIZE = new AtomicInteger(DEFAULT_BLOOM_FILTER_SIZE);

    public BloomFilterSettings(Settings settings) {
        // TODO: define index settings for this?
    }

    int getBloomFilterSizeInBytes() {
        return BLOOM_FILTER_SIZE.get();
    }

    int getBloomFilterSizeInBits() {
        return BLOOM_FILTER_SIZE.get() * Byte.SIZE;
    }

    boolean loadBloomFilterInMemory() {
        return LOAD_BLOOM_FILTER_IN_MEMORY.get();
    }
}
