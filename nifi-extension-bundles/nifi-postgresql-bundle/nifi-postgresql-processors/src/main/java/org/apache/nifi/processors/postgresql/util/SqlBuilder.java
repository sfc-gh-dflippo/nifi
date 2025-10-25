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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.postgresql.service.util.TableMetadata;

/**
 * Builds COPY SQL fragments for CSV and Parquet formats based on processor properties. Centralizes pg_parquet options mapping according to pg_parquet
 * docs: compression, parquet version, and match_by for COPY FROM.
 *
 * Source: CrunchyData pg_parquet docs https://github.com/CrunchyData/pg_parquet
 */
public final class SqlBuilder {
    private final ProcessorProperties properties;

    public SqlBuilder(final ProcessorProperties properties) {
        this.properties = properties;
    }

    private static final String CSV_FORMAT_WITH_HEADER = "FORMAT 'csv', HEADER true, DELIMITER '%s', QUOTE '%s', ESCAPE '%s', NULL '%s'";
    private static final String CSV_FORMAT_NO_HEADER = "FORMAT 'csv', HEADER false, DELIMITER '%s', QUOTE '%s', ESCAPE '%s', NULL '%s'";
    private static final String PARQUET_TO_FORMAT_BASE = "FORMAT 'parquet', compression 'snappy'";
    private static final String PARQUET_TO_FORMAT_WITH_VERSION = "FORMAT 'parquet', compression 'snappy', parquet_version '%s'";
    private static final String PARQUET_FROM_FORMAT = "FORMAT 'parquet', match_by '%s'";

    public String buildCopyToWith() {
        final String bulkTransferDataFormat = properties.getBulkTransferDataFormat();
        if (bulkTransferDataFormat.equalsIgnoreCase("CSV")) {
            final boolean hasHeader = "true".equalsIgnoreCase(properties.getCsvHeader());
            final String csvFormat = hasHeader ? CSV_FORMAT_WITH_HEADER : CSV_FORMAT_NO_HEADER;
            return String.format(csvFormat, properties.getCsvDelimiter(), properties.getCsvQuote(), properties.getCsvEscape(),
                    properties.getCsvNullToken());
        } else if (bulkTransferDataFormat.equalsIgnoreCase("Parquet")) {
            final String parquetVersion = properties.getParquetVersion();
            // Only include parquet_version if explicitly set (v1 or v2)
            // Some PostgreSQL servers don't support this option, so we leave it out by
            // default
            if (parquetVersion != null && !parquetVersion.isEmpty()) {
                return String.format(PARQUET_TO_FORMAT_WITH_VERSION, parquetVersion);
            } else {
                return PARQUET_TO_FORMAT_BASE;
            }
        }
        return "";
    }

    public String buildCopyFromWith() {
        final String bulkTransferDataFormat = properties.getBulkTransferDataFormat();
        if (bulkTransferDataFormat.equalsIgnoreCase("CSV")) {
            final boolean hasHeader = "true".equalsIgnoreCase(properties.getCsvHeader());
            final String csvFormat = hasHeader ? CSV_FORMAT_WITH_HEADER : CSV_FORMAT_NO_HEADER;
            return String.format(csvFormat, properties.getCsvDelimiter(), properties.getCsvQuote(), properties.getCsvEscape(),
                    properties.getCsvNullToken());
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
        return "COPY " + quoteIdentifierPath(tableName)
                + (targetColumnsCsv == null || targetColumnsCsv.isEmpty() ? "" : (" (" + targetColumnsCsv + ")")) + " FROM STDIN WITH ("
                + buildCopyFromWith() + ")";
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
                if (i > 0)
                    predicate.append(',');
                final String v = lastValues.get(maxColumns.get(i));
                predicate.append(literal(v));
            }
            predicate.append(")");
        }

        final String orderBy = maxColumns.isEmpty()
                ? ""
                : (" ORDER BY " + String.join(", ", maxColumns.stream().map(SqlBuilder::quoteIdentifier).toList()));
        return baseQuery + predicate + orderBy;
    }

    public List<String> getMaximumValueColumns() {
        return properties.getMaximumValueColumnsList();
    }

    public String buildSelectMaxSql(final String sql) {
        final List<String> maxColumns = getMaximumValueColumns();
        if (maxColumns.isEmpty())
            return null;
        return "SELECT " + String.join(", ", maxColumns.stream().map(c -> "max(\"" + c + "\") as \"" + c + "\"").toList()) + " FROM (" + sql + ")";
    }

    public FlowFile getWorkingFlowFile() {
        return properties.getWorkingFlowFile();
    }
    public boolean isPlaceholder() {
        return properties.isPlaceholder();
    }
    public void cleanup() {
        properties.cleanup();
    }

    private String getValueForDescriptor(final PropertyDescriptor descriptor) {
        if (descriptor == ProcessorProperties.CSV_DELIMITER)
            return properties.getCsvDelimiter();
        if (descriptor == ProcessorProperties.CSV_QUOTE)
            return properties.getCsvQuote();
        if (descriptor == ProcessorProperties.CSV_ESCAPE)
            return properties.getCsvEscape();
        if (descriptor == ProcessorProperties.CSV_NULL)
            return properties.getCsvNullToken();
        if (descriptor == ProcessorProperties.CSV_HEADER)
            return properties.getCsvHeader();
        if (descriptor == ProcessorProperties.PARQUET_VERSION)
            return properties.getParquetVersion();
        if (descriptor == ProcessorProperties.PARQUET_MATCH_BY)
            return properties.getParquetMatchBy();
        return null;
    }

    public static String quoteIdentifier(final String identifier) {
        if (identifier == null || identifier.isBlank())
            return identifier;
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    public static String quoteIdentifierPath(final String path) {
        if (path == null || path.isBlank())
            return path;
        final String[] parts = path.split("\\.");
        final StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            if (sb.length() > 0)
                sb.append('.');
            sb.append(quoteIdentifier(p.trim()));
        }
        return sb.toString();
    }

    public static String literal(final String value) {
        if (value == null)
            return "NULL";
        // Always return a quoted text literal and rely on PostgreSQL implicit casts
        // Values saved by MaxValueTracker use ISO 8601 for temporal types
        final String escaped = value.replace("'", "''");
        return "'" + escaped + "'";
    }

    /**
     * Generates a PostgreSQL upsert statement using INSERT...ON CONFLICT...DO UPDATE pattern based on
     * cached table metadata. Uses StringBuilder for PostgreSQL-specific syntax (ON CONFLICT, JSONB ||).
     * Optionally includes RETURNING clause with CTE for returning rows with JSONB casting.
     *
     * @param metadata
     *            Table metadata containing column info, primary keys, and JSONB columns
     * @param schema
     *            Target schema name
     * @param table
     *            Target table name
     * @param tempTable
     *            Temporary table name containing source data
     * @param returnRows
     *            If true, includes RETURNING clause with CTE and optional JSONB casting
     * @param castJsonbToText
     *            If true and returnRows is true, cast JSONB columns to TEXT in the final SELECT
     * @return Generated upsert SQL statement
     */
    public static String buildUpsertStatement(final TableMetadata metadata, final String schema, final String table, 
            final String tempTable, final boolean returnRows, final boolean castJsonbToText) {
        if (!metadata.hasPrimaryKey()) {
            throw new IllegalStateException("Table must have a primary key to generate upsert statement");
        }

        final List<String> allColumns = metadata.getAllColumns();
        final Set<String> pkColumns = metadata.getPrimaryKeyColumns();
        final Set<String> jsonbColumns = metadata.getJsonbColumns();

        final StringBuilder sql = new StringBuilder();
        
        // If returning rows, wrap in CTE for JSONB casting
        if (returnRows) {
            sql.append("WITH upserted AS (");
        }
        
        sql.append("INSERT INTO ").append(quoteIdentifierPath(schema + "." + table));
        sql.append(" SELECT * FROM ").append(quoteIdentifier(tempTable));
        sql.append(" ON CONFLICT (");

        // Add primary key columns for conflict detection
        boolean first = true;
        for (String pkCol : pkColumns) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(pkCol));
            first = false;
        }

        sql.append(") DO UPDATE SET ");

        // Build UPDATE SET clause with JSONB concatenation where needed
        first = true;
        for (String col : allColumns) {
            if (!metadata.isPrimaryKeyColumn(col)) {
                if (!first) {
                    sql.append(", ");
                }
                sql.append(quoteIdentifier(col)).append(" = ");

                // Use JSONB concatenation (||) for JSONB/JSON columns
                if (metadata.isJsonbColumn(col)) {
                    // Merge JSONB: target_table.column || EXCLUDED.column (right side overrides left)
                    sql.append(quoteIdentifierPath(schema + "." + table)).append(".").append(quoteIdentifier(col));
                    sql.append(" || ");
                    sql.append("EXCLUDED.").append(quoteIdentifier(col));
                } else {
                    sql.append("EXCLUDED.").append(quoteIdentifier(col));
                }
                first = false;
            }
        }

        // Only add RETURNING if we need rows back
        if (returnRows) {
            sql.append(" RETURNING *");
            sql.append(") SELECT ");

            // Build explicit column list with conditional CAST for JSONB
            first = true;
            for (String col : allColumns) {
                if (!first) {
                    sql.append(", ");
                }
                final String quotedCol = quoteIdentifier(col);
                // Cast JSONB to TEXT if requested (for Parquet exports)
                if (castJsonbToText && jsonbColumns.contains(col)) {
                    sql.append("CAST(").append(quotedCol).append(" AS TEXT) AS ").append(quotedCol);
                } else {
                    sql.append(quotedCol);
                }
                first = false;
            }
            sql.append(" FROM upserted");
        }

        return sql.toString();
    }

    /**
     * Wraps a query to cast JSONB columns to TEXT. Fetches column list from the table/view
     * and either expands SELECT * or wraps the query with a SELECT that casts JSONB columns.
     * Used for both Parquet and CSV exports to ensure consistent text representation.
     *
     * @param connection
     *            Database connection
     * @param tableName
     *            Table or view name (used to fetch column list)
     * @param baseQuery
     *            Base query to wrap
     * @param jsonbColumns
     *            List of JSONB column names to cast
     * @return Wrapped query with JSONB casting, or original query on error
     */
    public static String wrapQueryWithJsonbCasting(final Connection connection, final String tableName, 
            final String baseQuery, final List<String> jsonbColumns) {
        // Fetch all column names in order from the table/view
        final List<String> allColumns = new ArrayList<>();
        final String sql = "SELECT a.attname FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid " +
                "WHERE c.relname = '" + tableName + "' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum";
        try (Statement st = connection.createStatement(); 
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                allColumns.add(rs.getString(1));
            }
        } catch (Exception e) {
            // On error, return original query
            return baseQuery;
        }

        // Check if query contains SELECT * - if so, replace with explicit column list
        if (baseQuery.toUpperCase().contains("SELECT *")) {
            return expandSelectStarWithJsonbCasting(baseQuery, allColumns, jsonbColumns);
        }

        // Query doesn't have SELECT *, wrap it
        return wrapWithSelectAndJsonbCasting(allColumns, jsonbColumns, baseQuery);
    }

    /**
     * Expands SELECT * to explicit column list with CAST() for JSONB columns using string replacement
     */
    private static String expandSelectStarWithJsonbCasting(final String baseQuery, final List<String> allColumns, 
            final List<String> jsonbColumns) {
        final StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < allColumns.size(); i++) {
            if (i > 0) {
                columnList.append(", ");
            }
            final String col = allColumns.get(i);
            final String quotedCol = quoteIdentifier(col);
            if (jsonbColumns.contains(col)) {
                columnList.append("CAST(").append(quotedCol).append(" AS TEXT) AS ").append(quotedCol);
            } else {
                columnList.append(quotedCol);
            }
        }

        // Replace SELECT * with the explicit column list
        return baseQuery.replaceAll("(?i)SELECT\\s+\\*", "SELECT " + columnList.toString());
    }

    /**
     * Wraps query with SELECT that casts JSONB columns
     */
    private static String wrapWithSelectAndJsonbCasting(final List<String> allColumns, 
            final List<String> jsonbColumns, final String baseQuery) {
        final StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < allColumns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            final String col = allColumns.get(i);
            final String quotedCol = quoteIdentifier(col);
            if (jsonbColumns.contains(col)) {
                sb.append("CAST(").append(quotedCol).append(" AS TEXT) AS ").append(quotedCol);
            } else {
                sb.append(quotedCol);
            }
        }
        sb.append(" FROM (").append(baseQuery).append(") AS _base_query");
        return sb.toString();
    }

    /**
     * Creates the jsonb_merge_agg custom aggregate function if it doesn't already exist.
     * This function merges multiple JSONB objects chronologically using the || operator.
     * Thread-safe: uses CREATE OR REPLACE and handles concurrent creation attempts.
     * 
     * @param connection Database connection
     * @throws Exception if creation fails for reasons other than already existing
     */
    public static void ensureJsonbMergeAggExists(final Connection connection) throws Exception {
        // Try to create the aggregate function using CREATE OR REPLACE for idempotency
        // Note: CREATE OR REPLACE AGGREGATE requires PostgreSQL 11+
        // For older versions, we catch the "already exists" error
        final String createSql = "DO $$\n" +
                "BEGIN\n" +
                "    IF NOT EXISTS (\n" +
                "        SELECT 1 FROM pg_proc p\n" +
                "        JOIN pg_namespace n ON p.pronamespace = n.oid\n" +
                "        WHERE p.proname = 'jsonb_merge_agg'\n" +
                "        AND n.nspname = current_schema()\n" +
                "    ) THEN\n" +
                "        CREATE AGGREGATE jsonb_merge_agg(jsonb) (\n" +
                "            SFUNC = jsonb_concat,\n" +
                "            STYPE = jsonb,\n" +
                "            INITCOND = '{}'\n" +
                "        );\n" +
                "    END IF;\n" +
                "END$$;";
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSql);
        } catch (SQLException e) {
            // If the error is "already exists", ignore it (race condition with another thread)
            // Error code 42723 is "duplicate function" in PostgreSQL
            if (!"42723".equals(e.getSQLState())) {
                throw e;
            }
            // Otherwise, the function already exists, which is fine
        }
    }

    /**
     * Generates a PostgreSQL upsert statement with deduplication using array_agg aggregation pattern.
     * Groups records by primary key and uses the timestamp column for ordering. For JSONB columns,
     * uses jsonb_merge_agg for chronological merging.
     * 
     * @param metadata Table metadata containing column info, primary keys, and JSONB columns
     * @param schema Target schema name
     * @param table Target table name
     * @param tempTable Temporary table name containing source data
     * @param timestampColumn Column name to use for ordering records chronologically
     * @param returnRows If true, includes RETURNING clause with CTE and optional JSONB casting
     * @param castJsonbToText If true and returnRows is true, cast JSONB columns to TEXT in the final SELECT
     * @return Generated upsert SQL statement with deduplication
     */
    public static String buildUpsertStatementWithDeduplication(final TableMetadata metadata, final String schema, 
            final String table, final String tempTable, final String timestampColumn, 
            final boolean returnRows, final boolean castJsonbToText) {
        if (!metadata.hasPrimaryKey()) {
            throw new IllegalStateException("Table must have a primary key to generate upsert statement");
        }

        final List<String> allColumns = metadata.getAllColumns();
        final Set<String> pkColumns = metadata.getPrimaryKeyColumns();
        final Set<String> jsonbColumns = metadata.getJsonbColumns();

        final StringBuilder sql = new StringBuilder();
        
        // Start with CTE for aggregation/deduplication
        sql.append("WITH deduplicated AS (");
        sql.append(" SELECT ");
        
        // Build column list with aggregation
        boolean first = true;
        for (String col : allColumns) {
            if (!first) {
                sql.append(", ");
            }
            
            // Primary key columns: GROUP BY these, no aggregation needed
            if (metadata.isPrimaryKeyColumn(col)) {
                sql.append(quoteIdentifier(col));
            } else if (jsonbColumns.contains(col)) {
                // JSONB columns: use jsonb_merge_agg ordered ASC (chronologically)
                sql.append("jsonb_merge_agg(").append(quoteIdentifier(col));
                sql.append(" ORDER BY ").append(quoteIdentifier(timestampColumn)).append(" ASC) AS ");
                sql.append(quoteIdentifier(col));
            } else {
                // Scalar columns: pick most recent value (DESC = latest first, [1] = first element)
                sql.append("(array_agg(").append(quoteIdentifier(col));
                sql.append(" ORDER BY ").append(quoteIdentifier(timestampColumn)).append(" DESC))[1] AS ");
                sql.append(quoteIdentifier(col));
            }
            first = false;
        }
        
        sql.append(" FROM ").append(quoteIdentifier(tempTable));
        sql.append(" GROUP BY ");
        
        // Group by primary key columns
        first = true;
        for (String pkCol : pkColumns) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(pkCol));
            first = false;
        }
        sql.append(")");
        
        // If returning rows, wrap INSERT in another CTE for JSONB casting
        if (returnRows) {
            sql.append(", upserted AS (");
        }
        
        sql.append(" INSERT INTO ").append(quoteIdentifierPath(schema + "." + table));
        sql.append(" SELECT * FROM deduplicated");
        sql.append(" ON CONFLICT (");

        // Add primary key columns for conflict detection
        first = true;
        for (String pkCol : pkColumns) {
            if (!first) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(pkCol));
            first = false;
        }

        sql.append(") DO UPDATE SET ");

        // Build UPDATE SET clause with JSONB concatenation where needed
        first = true;
        for (String col : allColumns) {
            if (!metadata.isPrimaryKeyColumn(col)) {
                if (!first) {
                    sql.append(", ");
                }
                sql.append(quoteIdentifier(col)).append(" = ");

                // Use JSONB concatenation (||) for JSONB/JSON columns
                if (metadata.isJsonbColumn(col)) {
                    // Merge JSONB: target_table.column || EXCLUDED.column (right side overrides left)
                    sql.append(quoteIdentifierPath(schema + "." + table)).append(".").append(quoteIdentifier(col));
                    sql.append(" || ");
                    sql.append("EXCLUDED.").append(quoteIdentifier(col));
                } else {
                    sql.append("EXCLUDED.").append(quoteIdentifier(col));
                }
                first = false;
            }
        }

        // Only add RETURNING if we need rows back
        if (returnRows) {
            sql.append(" RETURNING *");
            sql.append(") SELECT ");

            // Build explicit column list with conditional CAST for JSONB
            first = true;
            for (String col : allColumns) {
                if (!first) {
                    sql.append(", ");
                }
                final String quotedCol = quoteIdentifier(col);
                // Cast JSONB to TEXT if requested (for Parquet exports)
                if (castJsonbToText && jsonbColumns.contains(col)) {
                    sql.append("CAST(").append(quotedCol).append(" AS TEXT) AS ").append(quotedCol);
                } else {
                    sql.append(quotedCol);
                }
                first = false;
            }
            sql.append(" FROM upserted");
        }

        return sql.toString();
    }
}
