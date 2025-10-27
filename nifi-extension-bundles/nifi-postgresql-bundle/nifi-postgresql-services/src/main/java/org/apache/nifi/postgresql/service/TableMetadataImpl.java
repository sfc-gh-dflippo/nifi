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

package org.apache.nifi.postgresql.service;

import org.apache.nifi.processors.postgresql.util.TableMetadata;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of TableMetadata for PostgreSQL tables.
 * Holds metadata about a PostgreSQL table including columns, primary keys, and JSONB columns.
 * 
 * Package-private implementation. Use PostgreSQLConnectionProviderService.getTableMetadata() instead.
 */
class TableMetadataImpl implements TableMetadata {
    private final String database;
    private final String schema;
    private final String table;
    private final List<String> allColumns;
    private final Map<String, String> columnTypes; // columnName -> typeName
    private final Set<String> primaryKeyColumns;
    private final Set<String> jsonbColumns;

    public TableMetadataImpl(final String database, final String schema, final String table, final List<String> allColumns,
            final Map<String, String> columnTypes, final Set<String> primaryKeyColumns, final Set<String> jsonbColumns) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.allColumns = allColumns;
        this.columnTypes = columnTypes;
        this.primaryKeyColumns = primaryKeyColumns;
        this.jsonbColumns = jsonbColumns;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public List<String> getAllColumns() {
        return allColumns;
    }

    @Override
    public Map<String, String> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public String getColumnType(final String columnName) {
        return columnTypes.get(columnName);
    }

    @Override
    public Set<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    @Override
    public Set<String> getJsonbColumns() {
        return jsonbColumns;
    }

    @Override
    public boolean hasPrimaryKey() {
        return !primaryKeyColumns.isEmpty();
    }

    @Override
    public boolean isJsonbColumn(final String columnName) {
        return jsonbColumns.contains(columnName);
    }

    @Override
    public boolean isPrimaryKeyColumn(final String columnName) {
        return primaryKeyColumns.contains(columnName);
    }

    @Override
    public boolean hasAllColumns(final List<String> requiredColumns) {
        final Set<String> tableColumnsLower = allColumns.stream().map(String::toLowerCase).collect(Collectors.toSet());
        return requiredColumns.stream().map(String::toLowerCase).allMatch(tableColumnsLower::contains);
    }

    /**
     * Fetch table metadata from PostgreSQL including columns, primary key, and JSONB columns
     *
     * @param connection
     *            Database connection
     * @param schema
     *            Schema name
     * @param table
     *            Table name
     * @return TableMetadata instance
     * @throws Exception
     *             if metadata fetch fails
     */
    public static TableMetadata fetch(final Connection connection, final String schema, final String table) throws Exception {
        // Get current database name
        final String database;
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery("SELECT current_database()")) {
            rs.next();
            database = rs.getString(1);
        }

        final List<String> allColumns = new ArrayList<>();
        final Map<String, String> columnTypes = new HashMap<>();
        final Set<String> primaryKeyColumns = new HashSet<>();
        final Set<String> jsonbColumns = new HashSet<>();

        // Query all columns and their types
        final String columnQuery = "SELECT a.attname, t.typname " + "FROM pg_attribute a " + "JOIN pg_class c ON a.attrelid = c.oid "
                + "JOIN pg_namespace n ON c.relnamespace = n.oid " + "JOIN pg_type t ON a.atttypid = t.oid " + "WHERE n.nspname = '" + schema
                + "' AND c.relname = '" + table + "' " + "AND a.attnum > 0 AND NOT a.attisdropped " + "ORDER BY a.attnum";

        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(columnQuery)) {
            while (rs.next()) {
                final String columnName = rs.getString(1);
                final String typeName = rs.getString(2);
                allColumns.add(columnName);
                columnTypes.put(columnName, typeName);
                if ("jsonb".equalsIgnoreCase(typeName) || "json".equalsIgnoreCase(typeName)) {
                    jsonbColumns.add(columnName);
                }
            }
        }

        // Query primary key columns
        final String pkQuery = "SELECT a.attname " + "FROM pg_index i "
                + "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " + "JOIN pg_class c ON c.oid = i.indrelid "
                + "JOIN pg_namespace n ON n.oid = c.relnamespace " + "WHERE i.indisprimary " + "AND n.nspname = '" + schema + "' AND c.relname = '"
                + table + "'";

        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(pkQuery)) {
            while (rs.next()) {
                primaryKeyColumns.add(rs.getString(1));
            }
        }

        return new TableMetadataImpl(database, schema, table, allColumns, columnTypes, primaryKeyColumns, jsonbColumns);
    }

}

