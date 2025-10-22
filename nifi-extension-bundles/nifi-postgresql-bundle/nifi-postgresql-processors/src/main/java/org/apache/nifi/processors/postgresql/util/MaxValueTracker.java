/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.postgresql.util;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks maximum values for a configured set of columns while iterating records.
 * Values are compared using natural Comparable ordering. Nulls are ignored.
 */
public final class MaxValueTracker {
    private final List<String> columns;
    private final Map<String, Object> currentMax = new HashMap<>();

    public MaxValueTracker(final List<String> columns) {
        this.columns = columns == null ? java.util.Collections.emptyList() : columns;
    }

    public void observe(final Record record) {
        if (record == null || columns.isEmpty()) return;
        for (String col : columns) {
            final Object value = record.getValue(col);
            updateMax(col, value);
        }
    }

    public void observe(final Map<String, Object> valuesByColumn) {
        if (valuesByColumn == null || columns.isEmpty()) return;
        for (String col : columns) {
            updateMax(col, valuesByColumn.get(col));
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void updateMax(final String column, final Object value) {
        if (value == null) return;
        final Object prior = currentMax.get(column);
        if (prior == null) {
            currentMax.put(column, value);
            return;
        }
        if (prior instanceof Comparable && value instanceof Comparable) {
            try {
                final Comparable p = (Comparable) prior;
                final Comparable v = (Comparable) value;
                if (p.compareTo(v) < 0) {
                    currentMax.put(column, value);
                }
            } catch (ClassCastException ignore) {
                // Types not comparable; fall back to String compare
                if (prior.toString().compareTo(value.toString()) < 0) {
                    currentMax.put(column, value);
                }
            }
        } else {
            // Fallback to String compare
            if (prior.toString().compareTo(value.toString()) < 0) {
                currentMax.put(column, value);
            }
        }
    }

    public Map<String, String> getMaxValuesAsStrings() {
        final Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, Object> e : currentMax.entrySet()) {
            if (e.getValue() != null) out.put(e.getKey(), e.getValue().toString());
        }
        return out;
    }

    public void reset() { currentMax.clear(); }
}


