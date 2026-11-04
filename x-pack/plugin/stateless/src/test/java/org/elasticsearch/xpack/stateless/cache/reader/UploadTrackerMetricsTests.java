/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.test.ESTestCase;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;

public class UploadTrackerMetricsTests extends ESTestCase {

    // The pattern enforced by the APM meter registry when a metric is registered (see MetricValidator in the apm module), which is not
    // available here: the RecordingMeterRegistry used by the other tests does not validate names.
    private static final Pattern METRIC_NAME_PATTERN = Pattern.compile(
        "es(\\.[a-z][a-z0-9_]{0,29}){1,8}\\.(ratio|duration|current|total|status|utilization|usage|size|histogram|time)"
    );

    public void testMetricNamesAreValid() throws IllegalAccessException {
        final var invalid = new ArrayList<String>();
        int checked = 0;
        for (var field : UploadTrackerMetrics.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getName().endsWith("_METRIC")) {
                final var name = (String) field.get(null);
                checked++;
                if (METRIC_NAME_PATTERN.matcher(name).matches() == false) {
                    invalid.add(name);
                }
            }
        }
        assertThat(checked, greaterThan(0));
        assertThat(invalid, empty());
    }
}
