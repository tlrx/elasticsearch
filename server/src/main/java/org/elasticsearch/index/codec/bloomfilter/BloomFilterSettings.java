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

public class BloomFilterSettings {
    private final int DEFAULT_BLOOM_FILTER_SIZE = Math.toIntExact(ByteSizeValue.ofKb(128).getBytes());
    public static final BloomFilterSettings DEFAULT_BLOOM_FILTER_SETTINGS = new BloomFilterSettings(Settings.EMPTY);

    public BloomFilterSettings(Settings settings) {

    }

    int getBloomFilterSizeInBytes() {
        return DEFAULT_BLOOM_FILTER_SIZE;
    }

    int getBloomFilterSizeInBits() {
        return DEFAULT_BLOOM_FILTER_SIZE * Byte.SIZE;
    }

    boolean loadBloomFilterInMemory() {
        return true;
    }
}
