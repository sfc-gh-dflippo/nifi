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

import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.processors.postgresql.util.TableMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that validate TableMetadata fetching from a real PostgreSQL database.
 * Tests the PostgreSQLConnectionProviderService.getTableMetadata() method with various table structures.
 */
public class TableMetadataFetchIT {

    private PostgreSQLTestHelper testHelper;
    private PostgreSQLConnectionProviderService connectionService;
    private Connection connection;

    @BeforeEach
    public void setup() throws Exception {
        testHelper = PostgreSQLTestHelper.builder().build();
        testHelper.setup();
        
        connectionService = testHelper.getConnectionService();
        connection = testHelper.getConnection();
    }

    @AfterEach
    public void teardown() throws Exception {
        testHelper.teardown();
    }

    @Test
    public void testFetchMetadataSimpleTable() throws Exception {
        // Create a simple test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_simple_table CASCADE");
            stmt.execute("CREATE TABLE test_simple_table (" +
                    "id INTEGER PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "email VARCHAR(100)" +
                    ")");
            connection.commit();
        }

        // Fetch metadata using the service (tests the production code path with caching)
        TableMetadata metadata = connectionService.getTableMetadata("public", "test_simple_table");

        // Validate basic properties
        assertNotNull(metadata);
        assertEquals("public", metadata.getSchema(), "Schema name mismatch");
        assertEquals("test_simple_table", metadata.getTable(), "Table name mismatch");

        // Validate exact column list (order and names)
        List<String> columns = metadata.getAllColumns();
        assertEquals(3, columns.size(), "Column count mismatch");
        assertEquals("id", columns.get(0), "First column should be 'id'");
        assertEquals("name", columns.get(1), "Second column should be 'name'");
        assertEquals("email", columns.get(2), "Third column should be 'email'");

        // Validate column types
        Map<String, String> columnTypes = metadata.getColumnTypes();
        assertEquals(3, columnTypes.size(), "Column types map size mismatch");
        assertEquals("int4", columnTypes.get("id"), "id column type should be int4");
        assertEquals("varchar", columnTypes.get("name"), "name column type should be varchar");
        assertEquals("varchar", columnTypes.get("email"), "email column type should be varchar");

        // Validate getColumnType method
        assertEquals("int4", metadata.getColumnType("id"));
        assertEquals("varchar", metadata.getColumnType("name"));
        assertEquals("varchar", metadata.getColumnType("email"));
        assertNull(metadata.getColumnType("nonexistent"), "Non-existent column should return null");

        // Validate primary key - exact columns
        assertTrue(metadata.hasPrimaryKey(), "Should have primary key");
        Set<String> pkColumns = metadata.getPrimaryKeyColumns();
        assertEquals(1, pkColumns.size(), "Should have exactly 1 primary key column");
        assertTrue(pkColumns.contains("id"), "Primary key should contain 'id'");
        
        // Validate isPrimaryKeyColumn method - exact names
        assertTrue(metadata.isPrimaryKeyColumn("id"), "'id' should be primary key");
        assertFalse(metadata.isPrimaryKeyColumn("name"), "'name' should NOT be primary key");
        assertFalse(metadata.isPrimaryKeyColumn("email"), "'email' should NOT be primary key");
        assertFalse(metadata.isPrimaryKeyColumn("ID"), "Case sensitivity: 'ID' should NOT match 'id'");

        // Validate no JSONB columns
        Set<String> jsonbColumns = metadata.getJsonbColumns();
        assertTrue(jsonbColumns.isEmpty(), "Should have no JSONB columns");
        assertFalse(metadata.isJsonbColumn("id"));
        assertFalse(metadata.isJsonbColumn("name"));
        assertFalse(metadata.isJsonbColumn("email"));
    }

    @Test
    public void testFetchMetadataTableWithJsonbColumns() throws Exception {
        // Create table with JSONB columns
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_jsonb_table CASCADE");
            stmt.execute("CREATE TABLE test_jsonb_table (" +
                    "id INTEGER PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "metadata JSONB, " +
                    "settings JSON" +
                    ")");
            connection.commit();
        }

        // Fetch metadata using the service (tests the production code path with caching)
        TableMetadata metadata = connectionService.getTableMetadata("public", "test_jsonb_table");

        // Validate exact column list and order
        List<String> columns = metadata.getAllColumns();
        assertEquals(4, columns.size(), "Should have exactly 4 columns");
        assertEquals("id", columns.get(0), "First column should be 'id'");
        assertEquals("name", columns.get(1), "Second column should be 'name'");
        assertEquals("metadata", columns.get(2), "Third column should be 'metadata'");
        assertEquals("settings", columns.get(3), "Fourth column should be 'settings'");

        // Validate column types - especially JSONB vs JSON
        Map<String, String> columnTypes = metadata.getColumnTypes();
        assertEquals(4, columnTypes.size(), "Should have 4 column types");
        assertEquals("int4", columnTypes.get("id"), "id should be int4");
        assertEquals("varchar", columnTypes.get("name"), "name should be varchar");
        assertEquals("jsonb", columnTypes.get("metadata"), "metadata should be jsonb");
        assertEquals("json", columnTypes.get("settings"), "settings should be json");

        // Validate JSONB columns are detected (both JSONB and JSON types)
        Set<String> jsonbColumns = metadata.getJsonbColumns();
        assertEquals(2, jsonbColumns.size(), "Should detect 2 JSONB/JSON columns");
        assertTrue(jsonbColumns.contains("metadata"), "Should contain 'metadata'");
        assertTrue(jsonbColumns.contains("settings"), "Should contain 'settings'");
        
        // Validate isJsonbColumn method - exact names
        assertTrue(metadata.isJsonbColumn("metadata"), "'metadata' is JSONB");
        assertTrue(metadata.isJsonbColumn("settings"), "'settings' is JSON (treated as JSONB)");
        assertFalse(metadata.isJsonbColumn("id"), "'id' is NOT JSONB");
        assertFalse(metadata.isJsonbColumn("name"), "'name' is NOT JSONB");
        assertFalse(metadata.isJsonbColumn("METADATA"), "Case sensitivity: 'METADATA' should NOT match");

        // Validate primary key
        Set<String> pkColumns = metadata.getPrimaryKeyColumns();
        assertEquals(1, pkColumns.size(), "Should have 1 primary key column");
        assertTrue(pkColumns.contains("id"), "Primary key should be 'id'");
        assertTrue(metadata.isPrimaryKeyColumn("id"), "'id' should be primary key");
        assertFalse(metadata.isPrimaryKeyColumn("metadata"), "'metadata' should NOT be primary key");
    }

    @Test
    public void testMetadataCompositePrimaryKey() throws Exception {
        // Create table with composite primary key
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_composite_pk CASCADE");
            stmt.execute("CREATE TABLE test_composite_pk (" +
                    "tenant_id INTEGER, " +
                    "user_id INTEGER, " +
                    "name VARCHAR(100), " +
                    "email VARCHAR(100), " +
                    "PRIMARY KEY (tenant_id, user_id)" +
                    ")");
            connection.commit();
        }

        // Fetch metadata using the service (tests the production code path with caching)
        TableMetadata metadata = connectionService.getTableMetadata("public", "test_composite_pk");

        // Validate exact column list
        List<String> columns = metadata.getAllColumns();
        assertEquals(4, columns.size(), "Should have exactly 4 columns");
        assertEquals("tenant_id", columns.get(0), "First column should be 'tenant_id'");
        assertEquals("user_id", columns.get(1), "Second column should be 'user_id'");
        assertEquals("name", columns.get(2), "Third column should be 'name'");
        assertEquals("email", columns.get(3), "Fourth column should be 'email'");

        // Validate column types
        Map<String, String> columnTypes = metadata.getColumnTypes();
        assertEquals(4, columnTypes.size(), "Should have 4 column types");
        assertEquals("int4", columnTypes.get("tenant_id"), "tenant_id should be int4");
        assertEquals("int4", columnTypes.get("user_id"), "user_id should be int4");
        assertEquals("varchar", columnTypes.get("name"), "name should be varchar");
        assertEquals("varchar", columnTypes.get("email"), "email should be varchar");

        // Validate composite primary key - exact columns
        assertTrue(metadata.hasPrimaryKey(), "Should have composite primary key");
        Set<String> pkColumns = metadata.getPrimaryKeyColumns();
        assertEquals(2, pkColumns.size(), "Should have exactly 2 primary key columns");
        assertTrue(pkColumns.contains("tenant_id"), "Primary key should contain 'tenant_id'");
        assertTrue(pkColumns.contains("user_id"), "Primary key should contain 'user_id'");
        
        // Validate isPrimaryKeyColumn for all columns
        assertTrue(metadata.isPrimaryKeyColumn("tenant_id"), "'tenant_id' IS primary key");
        assertTrue(metadata.isPrimaryKeyColumn("user_id"), "'user_id' IS primary key");
        assertFalse(metadata.isPrimaryKeyColumn("name"), "'name' is NOT primary key");
        assertFalse(metadata.isPrimaryKeyColumn("email"), "'email' is NOT primary key");
        
        // Test case sensitivity
        assertFalse(metadata.isPrimaryKeyColumn("TENANT_ID"), "Case: 'TENANT_ID' should NOT match");
        assertFalse(metadata.isPrimaryKeyColumn("USER_ID"), "Case: 'USER_ID' should NOT match");
        assertFalse(metadata.isPrimaryKeyColumn("Tenant_Id"), "Case: 'Tenant_Id' should NOT match");
    }

    @Test
    public void testMetadataForComplexTableStructure() throws Exception {
        // This test validates metadata for a table structure with mixed column types
        // including JSONB columns that need TEXT casting during Parquet export
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_complex_table CASCADE");
            
            // Create table with mixed column types
            stmt.execute("CREATE TABLE test_complex_table (" +
                    "id BIGSERIAL PRIMARY KEY, " +
                    "user_id INTEGER NOT NULL, " +
                    "email VARCHAR(255), " +
                    "profile JSONB, " +  // JSONB column that needs TEXT casting
                    "preferences JSON, " +  // JSON column that needs TEXT casting
                    "tags TEXT[], " +  // Array column (not JSONB)
                    "created_at TIMESTAMP DEFAULT NOW(), " +
                    "updated_at TIMESTAMP" +
                    ")");
            
            // Insert test data
            stmt.execute("INSERT INTO test_complex_table " +
                    "(user_id, email, profile, preferences, tags) VALUES " +
                    "(1, 'user1@test.com', '{\"name\": \"User 1\"}', '{\"theme\": \"dark\"}', ARRAY['tag1', 'tag2'])");
            
            connection.commit();
        }

        // Fetch metadata using the service (tests the production code path with caching)
        TableMetadata metadata = connectionService.getTableMetadata("public", "test_complex_table");

        // Validate exact column list (8 columns in order)
        List<String> columns = metadata.getAllColumns();
        assertEquals(8, columns.size(), "Should have exactly 8 columns");
        assertEquals("id", columns.get(0), "Column 0 should be 'id'");
        assertEquals("user_id", columns.get(1), "Column 1 should be 'user_id'");
        assertEquals("email", columns.get(2), "Column 2 should be 'email'");
        assertEquals("profile", columns.get(3), "Column 3 should be 'profile'");
        assertEquals("preferences", columns.get(4), "Column 4 should be 'preferences'");
        assertEquals("tags", columns.get(5), "Column 5 should be 'tags'");
        assertEquals("created_at", columns.get(6), "Column 6 should be 'created_at'");
        assertEquals("updated_at", columns.get(7), "Column 7 should be 'updated_at'");

        // Validate exact column types
        Map<String, String> columnTypes = metadata.getColumnTypes();
        assertEquals(8, columnTypes.size(), "Should have 8 column types");
        assertEquals("int8", columnTypes.get("id"), "id (BIGSERIAL) should be int8");
        assertEquals("int4", columnTypes.get("user_id"), "user_id should be int4");
        assertEquals("varchar", columnTypes.get("email"), "email should be varchar");
        assertEquals("jsonb", columnTypes.get("profile"), "profile should be jsonb");
        assertEquals("json", columnTypes.get("preferences"), "preferences should be json");
        assertEquals("_text", columnTypes.get("tags"), "tags (TEXT[]) should be _text");
        assertEquals("timestamp", columnTypes.get("created_at"), "created_at should be timestamp");
        assertEquals("timestamp", columnTypes.get("updated_at"), "updated_at should be timestamp");

        // Validate JSONB columns are detected (these need TEXT casting for Parquet)
        Set<String> jsonbColumns = metadata.getJsonbColumns();
        assertEquals(2, jsonbColumns.size(), "Should detect both JSONB and JSON columns");
        assertTrue(jsonbColumns.contains("profile"), "Should contain 'profile'");
        assertTrue(jsonbColumns.contains("preferences"), "Should contain 'preferences'");
        
        // Validate isJsonbColumn for each column
        assertTrue(metadata.isJsonbColumn("profile"), "'profile' IS JSONB");
        assertTrue(metadata.isJsonbColumn("preferences"), "'preferences' IS JSON (treated as JSONB)");
        assertFalse(metadata.isJsonbColumn("id"), "'id' is NOT JSONB");
        assertFalse(metadata.isJsonbColumn("user_id"), "'user_id' is NOT JSONB");
        assertFalse(metadata.isJsonbColumn("email"), "'email' is NOT JSONB");
        assertFalse(metadata.isJsonbColumn("tags"), "'tags' (TEXT[]) is NOT JSONB");
        assertFalse(metadata.isJsonbColumn("created_at"), "'created_at' is NOT JSONB");
        assertFalse(metadata.isJsonbColumn("updated_at"), "'updated_at' is NOT JSONB");

        // Test case sensitivity on JSONB columns
        assertFalse(metadata.isJsonbColumn("PROFILE"), "Case: 'PROFILE' should NOT match");
        assertFalse(metadata.isJsonbColumn("Profile"), "Case: 'Profile' should NOT match");
        assertFalse(metadata.isJsonbColumn("PREFERENCES"), "Case: 'PREFERENCES' should NOT match");

        // Validate primary key
        assertTrue(metadata.hasPrimaryKey(), "Should have primary key");
        Set<String> pkColumns = metadata.getPrimaryKeyColumns();
        assertEquals(1, pkColumns.size(), "Should have 1 primary key column");
        assertTrue(pkColumns.contains("id"), "Primary key should be 'id'");
        assertTrue(metadata.isPrimaryKeyColumn("id"), "'id' IS primary key");
        assertFalse(metadata.isPrimaryKeyColumn("user_id"), "'user_id' is NOT primary key");
        assertFalse(metadata.isPrimaryKeyColumn("ID"), "Case: 'ID' should NOT match");

        System.out.println("✓ Metadata correctly identifies all columns with exact types:");
        for (String col : columns) {
            String type = columnTypes.get(col);
            boolean isJsonb = metadata.isJsonbColumn(col);
            boolean isPk = metadata.isPrimaryKeyColumn(col);
            System.out.println("  - " + col + ": " + type + 
                    (isJsonb ? " [NEEDS TEXT CAST]" : "") +
                    (isPk ? " [PRIMARY KEY]" : ""));
        }
    }
}

