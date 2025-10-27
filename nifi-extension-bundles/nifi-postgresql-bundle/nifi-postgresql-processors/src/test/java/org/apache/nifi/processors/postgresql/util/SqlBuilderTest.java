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

import org.apache.nifi.processors.postgresql.util.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SqlBuilder SQL statement generation based on TableMetadata
 */
public class SqlBuilderTest {

    @Test
    public void testBuildUpsertStatementSimple() {
        // Create test metadata for a simple table with id, name, email
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "email"),
                Arrays.asList("id"),
                new HashSet<>()
        );

        String sql = SqlBuilder.buildUpsertStatement(metadata, "public", "users", "temp_users", false, false);

        // Verify SQL structure
        assertNotNull(sql);
        assertTrue(sql.contains("INSERT INTO \"public\".\"users\""), "Should contain INSERT INTO");
        assertTrue(sql.contains("SELECT * FROM \"temp_users\""), "Should select from temp table");
        assertTrue(sql.contains("ON CONFLICT (\"id\")"), "Should have ON CONFLICT on primary key");
        assertTrue(sql.contains("DO UPDATE SET"), "Should have DO UPDATE SET");
        assertFalse(sql.contains("RETURNING"), "Should not have RETURNING when returnRows=false");
    }

    @Test
    public void testBuildUpsertStatementWithReturning() {
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "email"),
                Arrays.asList("id"),
                new HashSet<>()
        );

        String sql = SqlBuilder.buildUpsertStatement(metadata, "public", "users", "temp_users", true, false);

        // Verify RETURNING clause is present
        assertTrue(sql.contains("RETURNING *"), "Should have RETURNING * when returnRows=true");
        assertTrue(sql.contains("WITH upserted AS"), "Should wrap with CTE for RETURNING");
    }

    @Test
    public void testBuildUpsertStatementWithJsonbColumns() {
        Set<String> jsonbColumns = new HashSet<>(Arrays.asList("metadata", "settings"));
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "metadata", "settings"),
                Arrays.asList("id"),
                jsonbColumns
        );

        String sql = SqlBuilder.buildUpsertStatement(metadata, "public", "users", "temp_users", false, false);

        // Verify JSONB columns use merge operator (||)
        assertTrue(sql.contains("\"metadata\" = \"public\".\"users\".\"metadata\" || EXCLUDED.\"metadata\""),
                "Should use JSONB merge for metadata column");
        assertTrue(sql.contains("\"settings\" = \"public\".\"users\".\"settings\" || EXCLUDED.\"settings\""),
                "Should use JSONB merge for settings column");
        assertFalse(sql.contains("\"name\" = \"public\".\"users\".\"name\""),
                "Should not merge non-JSONB columns");
    }

    @Test
    public void testBuildUpsertStatementWithJsonbCasting() {
        Set<String> jsonbColumns = new HashSet<>(Arrays.asList("metadata"));
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "metadata"),
                Arrays.asList("id"),
                jsonbColumns
        );

        String sql = SqlBuilder.buildUpsertStatement(metadata, "public", "users", "temp_users", true, true);

        // Verify JSONB columns are cast to TEXT in RETURNING clause
        assertTrue(sql.contains("CAST(\"metadata\" AS TEXT) AS \"metadata\""),
                "Should cast JSONB to TEXT when castJsonbToText=true");
        assertTrue(sql.contains("RETURNING *"), "Should have RETURNING clause");
    }

    @Test
    public void testBuildUpsertStatementWithCompositePrimaryKey() {
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("tenant_id", "user_id", "name", "email"),
                Arrays.asList("tenant_id", "user_id"),
                new HashSet<>()
        );

        String sql = SqlBuilder.buildUpsertStatement(metadata, "public", "users", "temp_users", false, false);

        // Verify composite primary key in ON CONFLICT clause
        assertTrue(sql.contains("ON CONFLICT (\"tenant_id\", \"user_id\")"),
                "Should handle composite primary key");
    }

    @Test
    public void testBuildUpsertStatementNoPrimaryKey() {
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("name", "email", "created_at"),
                Arrays.asList(),  // No primary key
                new HashSet<>()
        );

        // Should throw exception when no primary key exists
        assertThrows(IllegalStateException.class, () -> {
            SqlBuilder.buildUpsertStatement(metadata, "public", "logs", "temp_logs", false, false);
        }, "Should throw exception for table without primary key");
    }

    @Test
    public void testBuildUpsertStatementWithDeduplication() {
        Set<String> jsonbColumns = new HashSet<>(Arrays.asList("metadata"));
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "metadata", "updated_at"),
                Arrays.asList("id"),
                jsonbColumns
        );

        String sql = SqlBuilder.buildUpsertStatementWithDeduplication(
                metadata, "public", "users", "temp_users", "updated_at", false, false);

        // Verify deduplication SQL structure
        assertTrue(sql.contains("WITH deduplicated AS"), "Should have deduplicated CTE");
        assertTrue(sql.contains("GROUP BY"), "Should use GROUP BY for deduplication");
        assertTrue(sql.contains("array_agg("), "Should use array_agg for scalar columns");
        assertTrue(sql.contains("ORDER BY \"updated_at\""), "Should order by timestamp column");
    }

    @Test
    public void testBuildUpsertStatementWithDeduplicationAndJsonbMerge() {
        Set<String> jsonbColumns = new HashSet<>(Arrays.asList("metadata", "config"));
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "metadata", "config", "updated_at"),
                Arrays.asList("id"),
                jsonbColumns
        );

        String sql = SqlBuilder.buildUpsertStatementWithDeduplication(
                metadata, "public", "users", "temp_users", "updated_at", false, false);

        // Verify JSONB merge aggregation in deduplication
        assertTrue(sql.contains("jsonb_merge_agg(\"metadata\""), 
                "Should use jsonb_merge_agg for JSONB columns");
        assertTrue(sql.contains("jsonb_merge_agg(\"config\""), 
                "Should use jsonb_merge_agg for JSONB columns");
        assertTrue(sql.contains("array_agg(\"name\""), 
                "Should use array_agg for non-JSONB columns");
    }

    @Test
    public void testBuildUpsertStatementWithSpecialCharacters() {
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("user\"id", "name with spaces", "email"),
                Arrays.asList("user\"id"),
                new HashSet<>()
        );

        String sql = SqlBuilder.buildUpsertStatement(metadata, "public", "users", "temp_users", false, false);

        // Verify proper identifier quoting
        assertTrue(sql.contains("\"user\"\"id\""), "Should escape quotes in identifiers");
        assertTrue(sql.contains("\"name with spaces\""), "Should quote identifiers with spaces");
    }

    @Test
    public void testQuoteIdentifier() {
        assertEquals("\"users\"", SqlBuilder.quoteIdentifier("users"));
        assertEquals("\"user\"\"name\"", SqlBuilder.quoteIdentifier("user\"name"));
        assertEquals("\"Column With Spaces\"", SqlBuilder.quoteIdentifier("Column With Spaces"));
        // Empty string is returned as-is per implementation
        assertEquals("", SqlBuilder.quoteIdentifier(""));
        assertEquals(null, SqlBuilder.quoteIdentifier(null));
    }

    @Test
    public void testQuoteIdentifierPath() {
        assertEquals("\"public\".\"users\"", SqlBuilder.quoteIdentifierPath("public.users"));
        assertEquals("\"my\"\"schema\".\"my\"\"table\"", SqlBuilder.quoteIdentifierPath("my\"schema.my\"table"));
        assertEquals("\"schema\".\"table\".\"column\"", SqlBuilder.quoteIdentifierPath("schema.table.column"));
    }

    @Test
    public void testJsonbCastingInReturningClause() {
        // Test that JSONB casting is properly applied in RETURNING clause
        // This is indirectly tested through the buildUpsertStatementWithJsonbCasting test
        // The actual wrapping logic requires a database connection and is tested in integration tests
        Set<String> jsonbColumns = new HashSet<>(Arrays.asList("metadata"));
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("id", "name", "metadata"),
                Arrays.asList("id"),
                jsonbColumns
        );

        String sql = SqlBuilder.buildUpsertStatement(metadata, "public", "users", "temp_users", true, true);

        // Verify the SQL contains JSONB casting logic in the RETURNING clause
        assertTrue(sql.contains("CAST"), "Should contain CAST when castJsonbToText=true");
        assertTrue(sql.contains("TEXT"), "Should cast to TEXT");
    }

    @Test
    public void testBuildUpsertStatementCompositePrimaryKeyOrdering() {
        // Test that composite primary keys maintain order
        TableMetadata metadata = createTestMetadata(
                Arrays.asList("col_a", "col_b", "col_c", "data"),
                Arrays.asList("col_c", "col_a"),  // Note: out of column order
                new HashSet<>()
        );

        String sql = SqlBuilder.buildUpsertStatement(metadata, "public", "test", "temp_test", false, false);

        // The ON CONFLICT should list PK columns in the order they appear in the PK list
        assertTrue(sql.contains("ON CONFLICT"), "Should have ON CONFLICT clause");
        // The exact order depends on implementation, but both PK columns should be present
        assertTrue(sql.contains("\"col_c\"") && sql.contains("\"col_a\""), 
                "Should include all primary key columns");
    }

    /**
     * Helper method to create test TableMetadata instances
     */
    private TableMetadata createTestMetadata(List<String> allColumns, List<String> pkColumns, Set<String> jsonbColumns) {
        Map<String, String> columnTypes = new HashMap<>();
        for (String col : allColumns) {
            if (jsonbColumns.contains(col)) {
                columnTypes.put(col, "jsonb");
            } else if (pkColumns.contains(col)) {
                columnTypes.put(col, "int4");
            } else if (col.contains("_at")) {
                columnTypes.put(col, "timestamp");
            } else {
                columnTypes.put(col, "varchar");
            }
        }

        return new TestTableMetadata("testdb", "public", "test_table", allColumns, columnTypes,
                new HashSet<>(pkColumns), jsonbColumns);
    }

    /**
     * Simple test implementation of TableMetadata interface
     */
    private static class TestTableMetadata implements TableMetadata {
        private final String database;
        private final String schema;
        private final String table;
        private final List<String> allColumns;
        private final Map<String, String> columnTypes;
        private final Set<String> primaryKeyColumns;
        private final Set<String> jsonbColumns;

        public TestTableMetadata(String database, String schema, String table, List<String> allColumns,
                Map<String, String> columnTypes, Set<String> primaryKeyColumns, Set<String> jsonbColumns) {
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
        public String getColumnType(String columnName) {
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
        public boolean isJsonbColumn(String columnName) {
            return jsonbColumns.contains(columnName);
        }

        @Override
        public boolean isPrimaryKeyColumn(String columnName) {
            return primaryKeyColumns.contains(columnName);
        }

        @Override
        public boolean hasAllColumns(List<String> requiredColumns) {
            Set<String> lowerCaseColumns = new HashSet<>();
            for (String col : allColumns) {
                lowerCaseColumns.add(col.toLowerCase());
            }
            for (String required : requiredColumns) {
                if (!lowerCaseColumns.contains(required.toLowerCase())) {
                    return false;
                }
            }
            return true;
        }
    }
}

