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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.serialization.record.Record;

/**
 * Tracks maximum values for a configured set of columns while iterating records. Values are compared using natural Comparable ordering. Nulls are
 * ignored.
 */
public final class MaxValueTracker {
    private final List<String> columns;
    private final Map<String, Object> currentMax = new HashMap<>();

    public MaxValueTracker(final List<String> columns) {
        this.columns = columns == null ? Collections.emptyList() : columns;
    }

    public void observe(final Record record) {
        if (record == null || columns.isEmpty())
            return;
        for (String col : columns) {
            final Object value = record.getValue(col);
            updateMax(col, value);
        }
    }

    public void observe(final Map<String, Object> valuesByColumn) {
        if (valuesByColumn == null || columns.isEmpty())
            return;
        for (String col : columns) {
            updateMax(col, valuesByColumn.get(col));
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void updateMax(final String column, final Object value) {
        if (value == null)
            return;
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
            if (e.getValue() != null)
                out.put(e.getKey(), formatValueForState(e.getValue()));
        }
        return out;
    }

    public void reset() {
        currentMax.clear();
    }

    private static String formatValueForState(final Object value) {
        // If value is already a temporal type, format to ISO 8601 (UTC)
        if (value instanceof Timestamp ts) {
            final Instant instant = ts.toInstant();
            return DateTimeFormatter.ISO_INSTANT.format(instant);
        }
        if (value instanceof Date date) {
            final Instant instant = date.toInstant();
            return DateTimeFormatter.ISO_INSTANT.format(instant);
        }
        if (value instanceof OffsetDateTime odt) {
            return odt.withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
        }
        if (value instanceof ZonedDateTime zdt) {
            return zdt.withZoneSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
        }
        if (value instanceof LocalDateTime ldt) {
            final Instant instant = ldt.toInstant(ZoneOffset.UTC);
            return DateTimeFormatter.ISO_INSTANT.format(instant);
        }
        if (value instanceof Instant inst) {
            return DateTimeFormatter.ISO_INSTANT.format(inst);
        }

        // Default: use toString() representation (numbers remain numeric text)
        return value.toString();
    }
}
