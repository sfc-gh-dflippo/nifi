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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds COPY SQL fragments for CSV and Parquet formats based on processor properties.
 * Centralizes pg_parquet options mapping according to pg_parquet docs: compression, 
 * parquet version, and match_by for COPY FROM.
 *
 * Source: CrunchyData pg_parquet docs
 * https://github.com/CrunchyData/pg_parquet
 */
public final class SqlBuilder {
    private final ProcessorProperties properties;

    public SqlBuilder(final ProcessorProperties properties) {
        this.properties = properties;
    }

    private static final String CSV_FORMAT_WITH_HEADER = "FORMAT 'csv', HEADER true, DELIMITER '%s', QUOTE '%s', ESCAPE '%s', NULL '%s'";
    private static final String CSV_FORMAT_NO_HEADER = "FORMAT 'csv', HEADER false, DELIMITER '%s', QUOTE '%s', ESCAPE '%s', NULL '%s'";
    private static final String PARQUET_TO_FORMAT = "FORMAT 'parquet', compression 'snappy', parquet_version '%s'";
    private static final String PARQUET_FROM_FORMAT = "FORMAT 'parquet', match_by '%s'";

    public String buildCopyToWith() {
        final String bulkTransferDataFormat = properties.getBulkTransferDataFormat();
        if (bulkTransferDataFormat.equalsIgnoreCase("CSV")) {
            final boolean hasHeader = "true".equalsIgnoreCase(properties.getCsvHeader());
            final String csvFormat = hasHeader ? CSV_FORMAT_WITH_HEADER : CSV_FORMAT_NO_HEADER;
            return String.format(csvFormat, properties.getCsvDelimiter(), properties.getCsvQuote(), properties.getCsvEscape(), properties.getCsvNullToken());
        } else if (bulkTransferDataFormat.equalsIgnoreCase("Parquet")) {
            return String.format(PARQUET_TO_FORMAT, properties.getParquetVersion());
        }
        return "";
    }

    public String buildCopyFromWith() {
        final String bulkTransferDataFormat = properties.getBulkTransferDataFormat();
        if (bulkTransferDataFormat.equalsIgnoreCase("CSV")) {
            final boolean hasHeader = "true".equalsIgnoreCase(properties.getCsvHeader());
            final String csvFormat = hasHeader ? CSV_FORMAT_WITH_HEADER : CSV_FORMAT_NO_HEADER;
            return String.format(csvFormat, properties.getCsvDelimiter(), properties.getCsvQuote(), properties.getCsvEscape(), properties.getCsvNullToken());
        } else if (bulkTransferDataFormat.equalsIgnoreCase("Parquet")) {
            return String.format(PARQUET_FROM_FORMAT, properties.getParquetMatchBy());
        }
        return "";
    }

    public String buildCopyIntoTableSql() {
        return buildCopyIntoTableSql(properties.getTargetTable(), null);
    }

    public String buildCopyIntoTableSql(final String targetColumnsCsv) {
        return buildCopyIntoTableSql(properties.getTargetTable(), targetColumnsCsv);
    }

    public String buildCopyIntoTableSql(final String tableName, final String targetColumnsCsv) {
        return "COPY " + quoteIdentifierPath(tableName) + (targetColumnsCsv == null || targetColumnsCsv.isEmpty() ? "" : (" (" + targetColumnsCsv + ")")) +
                " FROM STDIN WITH (" + buildCopyFromWith() + ")";
    }

    public String buildCopyFromStatementSql(final String sql) {
        return "COPY (" + sql + ") TO STDOUT WITH (" + buildCopyToWith() + ")";
    }

    public String buildEmptyViewFromStatementSql(final String viewName, final String sql) {
        return "CREATE TEMPORARY VIEW " + quoteIdentifierPath(viewName) + " AS " + sql + " LIMIT 0";
    }

    public String buildBaseQuery() {
        final String customQuery = properties.getCustomQuery();
        final String sourceTable = properties.getSourceTable();
        if ((customQuery == null || customQuery.isBlank()) && (sourceTable == null || sourceTable.isBlank())) {
            throw new ProcessException("Either 'Custom Query' or 'Table Name' must be set");
        }

        final String baseQuery = (customQuery != null && !customQuery.isBlank())
                ? "SELECT * FROM (" + customQuery + ")"
                : "SELECT * FROM " + quoteIdentifierPath(sourceTable);

        final List<String> maxColumns = properties.getMaximumValueColumnsList();
        final Map<String, String> lastValues = properties.getLastMaxValuesFromState();
        final StringBuilder predicate = new StringBuilder();
        if (!maxColumns.isEmpty() && !lastValues.isEmpty() && lastValues.size() == maxColumns.size()) {
            predicate.append(" WHERE (");
            predicate.append(String.join(",", maxColumns.stream().map(SqlBuilder::quoteIdentifier).toList()));
            predicate.append(") > (");
            for (int i = 0; i < maxColumns.size(); i++) {
                if (i > 0) predicate.append(',');
                final String v = lastValues.get(maxColumns.get(i));
                predicate.append(literal(v));
            }
            predicate.append(")");
        }

        final String orderBy = maxColumns.isEmpty() ? "" : (" ORDER BY " + String.join(", ", maxColumns.stream().map(SqlBuilder::quoteIdentifier).toList()));
        return baseQuery + predicate + orderBy;
    }

    public List<String> getMaximumValueColumns() { return properties.getMaximumValueColumnsList(); }

    public String buildSelectMaxSql(final String sql) {
        final List<String> maxColumns = getMaximumValueColumns();
        if (maxColumns.isEmpty()) return null;
        return "SELECT " + String.join(", ", maxColumns.stream().map(c -> "max(\"" + c + "\") as \"" + c + "\"").toList()) +
                " FROM (" + sql + ")";
    }

    public FlowFile getWorkingFlowFile() { return properties.getWorkingFlowFile(); }
    public boolean isPlaceholder() { return properties.isPlaceholder(); }
    public void cleanup() { properties.cleanup(); }

    private String getValueForDescriptor(final PropertyDescriptor descriptor) {
        if (descriptor == ProcessorProperties.CSV_DELIMITER) return properties.getCsvDelimiter();
        if (descriptor == ProcessorProperties.CSV_QUOTE) return properties.getCsvQuote();
        if (descriptor == ProcessorProperties.CSV_ESCAPE) return properties.getCsvEscape();
        if (descriptor == ProcessorProperties.CSV_NULL) return properties.getCsvNullToken();
        if (descriptor == ProcessorProperties.CSV_HEADER) return properties.getCsvHeader();
        if (descriptor == ProcessorProperties.PARQUET_VERSION) return properties.getParquetVersion();
        if (descriptor == ProcessorProperties.PARQUET_MATCH_BY) return properties.getParquetMatchBy();
        return null;
    }

    public static String quoteIdentifier(final String identifier) {
        if (identifier == null || identifier.isBlank()) return identifier;
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    public static String quoteIdentifierPath(final String path) {
        if (path == null || path.isBlank()) return path;
        final String[] parts = path.split("\\.");
        final StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            if (sb.length() > 0) sb.append('.');
            sb.append(quoteIdentifier(p.trim()));
        }
        return sb.toString();
    }

    public static String literal(final String value) {
        if (value == null) return "NULL";
        final String escaped = value.replace("'", "''");
        return "'" + escaped + "'";
    }
}


