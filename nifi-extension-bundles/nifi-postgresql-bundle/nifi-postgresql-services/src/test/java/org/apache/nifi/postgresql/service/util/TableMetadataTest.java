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
package org.apache.nifi.postgresql.service.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for TableMetadata class
 */
public class TableMetadataTest {

    @Test
    public void testHasPrimaryKey() {
        // Table with primary key
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "email"),
                Arrays.asList("id"),
                new HashSet<>()
        );
        assertTrue(metadata.hasPrimaryKey());

        // Table without primary key
        metadata = createTestMetadata(
                Arrays.asList("id", "name", "email"),
                Arrays.asList(),
                new HashSet<>()
        );
        assertFalse(metadata.hasPrimaryKey());
    }

    @Test
    public void testIsPrimaryKeyColumn() {
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "email"),
                Arrays.asList("id"),
                new HashSet<>()
        );

        assertTrue(metadata.isPrimaryKeyColumn("id"));
        assertFalse(metadata.isPrimaryKeyColumn("name"));
        assertFalse(metadata.isPrimaryKeyColumn("email"));
    }

    @Test
    public void testIsJsonbColumn() {
        Set<String> jsonbColumns = new HashSet<>(Arrays.asList("metadata", "settings"));
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "metadata", "settings"),
                Arrays.asList("id"),
                jsonbColumns
        );

        assertFalse(metadata.isJsonbColumn("id"));
        assertFalse(metadata.isJsonbColumn("name"));
        assertTrue(metadata.isJsonbColumn("metadata"));
        assertTrue(metadata.isJsonbColumn("settings"));
    }

    @Test
    public void testHasAllColumnsMatch() {
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "email"),
                Arrays.asList("id"),
                new HashSet<>()
        );

        // All columns exist
        assertTrue(metadata.hasAllColumns(Arrays.asList("id", "name")));
        assertTrue(metadata.hasAllColumns(Arrays.asList("id", "name", "email")));

        // Case insensitive
        assertTrue(metadata.hasAllColumns(Arrays.asList("ID", "NAME")));
        assertTrue(metadata.hasAllColumns(Arrays.asList("Id", "Name", "Email")));
    }

    @Test
    public void testHasAllColumnsMismatch() {
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "email"),
                Arrays.asList("id"),
                new HashSet<>()
        );

        // Missing column
        assertFalse(metadata.hasAllColumns(Arrays.asList("id", "name", "phone")));
        assertFalse(metadata.hasAllColumns(Arrays.asList("address")));
    }

    // Note: Upsert SQL generation tests removed - SQL generation is SqlBuilder's responsibility, not TableMetadata's

    @Test
    public void testGetters() {
        Set<String> jsonbColumns = new HashSet<>(Arrays.asList("metadata"));
        Map<String, String> columnTypes = new HashMap<>();
        columnTypes.put("id", "int4");
        columnTypes.put("name", "varchar");
        columnTypes.put("metadata", "jsonb");

        TableMetadata metadata = new TableMetadata(
                "testdb",
                "public",
                "users",
                Arrays.asList("id", "name", "metadata"),
                columnTypes,
                new HashSet<>(Arrays.asList("id")),
                jsonbColumns
        );

        assertEquals(Arrays.asList("id", "name", "metadata"), metadata.getAllColumns());
        assertEquals(new HashSet<>(Arrays.asList("id")), metadata.getPrimaryKeyColumns());
        assertEquals(jsonbColumns, metadata.getJsonbColumns());
        assertEquals(columnTypes, metadata.getColumnTypes());
    }

    @Test
    public void testQuoteIdentifierWithSpecialChars() {
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("user\"name", "email"),
                Arrays.asList("user\"name"),
                new HashSet<>()
        );

        // Note: SQL generation moved to SqlBuilder
        // Identifier quoting is now tested in SqlBuilder tests
    }

    private TableMetadata createTestMetadata(List<String> allColumns, List<String> pkColumns, Set<String> jsonbColumns) {
        Map<String, String> columnTypes = new HashMap<>();
        for (String col : allColumns) {
            if (jsonbColumns.contains(col)) {
                columnTypes.put(col, "jsonb");
            } else if (pkColumns.contains(col)) {
                columnTypes.put(col, "int4");
            } else {
                columnTypes.put(col, "varchar");
            }
        }

        return new TableMetadata(
                "testdb",
                "public",
                "test_table",
                allColumns,
                columnTypes,
                new HashSet<>(pkColumns),
                jsonbColumns
        );
    }
}

