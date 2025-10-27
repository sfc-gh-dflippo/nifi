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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface for PostgreSQL table metadata including columns, primary keys, and JSONB columns.
 * Used for caching to avoid repeated metadata queries.
 */
public interface TableMetadata {

    /**
     * Gets the database name.
     *
     * @return the database name
     */
    String getDatabase();

    /**
     * Gets the schema name.
     *
     * @return the schema name
     */
    String getSchema();

    /**
     * Gets the table name.
     *
     * @return the table name
     */
    String getTable();

    /**
     * Gets all column names.
     *
     * @return list of all column names
     */
    List<String> getAllColumns();

    /**
     * Gets a map of column names to their PostgreSQL type names.
     *
     * @return map of column name to type name
     */
    Map<String, String> getColumnTypes();

    /**
     * Gets the PostgreSQL type name for a specific column.
     *
     * @param columnName the column name
     * @return the type name, or null if column doesn't exist
     */
    String getColumnType(String columnName);

    /**
     * Gets the set of primary key column names.
     *
     * @return set of primary key column names
     */
    Set<String> getPrimaryKeyColumns();

    /**
     * Gets the set of JSONB/JSON column names.
     *
     * @return set of JSONB/JSON column names
     */
    Set<String> getJsonbColumns();

    /**
     * Checks if the table has a primary key.
     *
     * @return true if table has a primary key
     */
    boolean hasPrimaryKey();

    /**
     * Checks if a column is a JSONB or JSON type.
     *
     * @param columnName the column name to check
     * @return true if the column is JSONB or JSON type
     */
    boolean isJsonbColumn(String columnName);

    /**
     * Checks if a column is part of the primary key.
     *
     * @param columnName the column name to check
     * @return true if the column is part of the primary key
     */
    boolean isPrimaryKeyColumn(String columnName);

    /**
     * Check if table has all specified columns (case-insensitive).
     *
     * @param requiredColumns columns to check
     * @return true if all columns exist in table
     */
    boolean hasAllColumns(List<String> requiredColumns);
}

