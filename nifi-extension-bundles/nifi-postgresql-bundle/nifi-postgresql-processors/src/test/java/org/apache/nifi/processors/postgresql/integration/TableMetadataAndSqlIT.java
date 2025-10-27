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
package org.apache.nifi.processors.postgresql;

import org.apache.nifi.postgresql.service.util.PostgreSQLTestHelper;
import org.apache.nifi.processors.postgresql.util.SqlBuilder;
import org.apache.nifi.processors.postgresql.util.TableMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that validate SQL statement generation from TableMetadata
 * and execution of those statements against a real PostgreSQL database.
 * 
 * Tests the complete flow: fetch metadata -> generate SQL -> execute SQL
 * 
 * Note: Pure metadata fetching tests are in TableMetadataFetchIT in the services module.
 */
public class TableMetadataAndSqlIT {

    private PostgreSQLTestHelper testHelper;
    private PostgreSQLConnectionProviderService connectionService;
    private Connection connection;

    @BeforeEach
    public void setup() throws Exception {
        testHelper = PostgreSQLTestHelper.builder()
            .withProcessor(PostgreSQLBulkExport.class)
            .build();
        testHelper.setup();
        
        connectionService = testHelper.getConnectionService();
        connection = testHelper.getConnection();
    }

    @AfterEach
    public void teardown() throws Exception {
        testHelper.teardown();
    }

    @Test
    public void testGenerateAndExecuteUpsertSqlFromMetadata() throws Exception {
        // Create tables
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_target_table CASCADE");
            stmt.execute("DROP TABLE IF EXISTS test_temp_table CASCADE");
            
            stmt.execute("CREATE TABLE test_target_table (" +
                    "id INTEGER PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "email VARCHAR(100)" +
                    ")");
            
            stmt.execute("CREATE TABLE test_temp_table (" +
                    "id INTEGER, " +
                    "name VARCHAR(100), " +
                    "email VARCHAR(100)" +
                    ")");
            
            // Insert initial data into target
            stmt.execute("INSERT INTO test_target_table VALUES (1, 'Original', 'original@test.com')");
            
            // Insert update and new record into temp table
            stmt.execute("INSERT INTO test_temp_table VALUES (1, 'Updated', 'updated@test.com')");
            stmt.execute("INSERT INTO test_temp_table VALUES (2, 'New', 'new@test.com')");
            
            connection.commit();
        }

        // Fetch metadata using the service (tests the production code path with caching)
        TableMetadata metadata = connectionService.getTableMetadata("public", "test_target_table");

        // Generate upsert SQL
        String upsertSql = SqlBuilder.buildUpsertStatement(
                metadata, "public", "test_target_table", "test_temp_table", false, false);

        assertNotNull(upsertSql);
        assertTrue(upsertSql.contains("INSERT INTO"));
        assertTrue(upsertSql.contains("ON CONFLICT"));
        assertTrue(upsertSql.contains("DO UPDATE SET"));

        // Execute the generated SQL
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(upsertSql);
            connection.commit();
        }

        // Verify the results
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, name, email FROM test_target_table ORDER BY id")) {
            
            // First row should be updated
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Updated", rs.getString("name"));
            assertEquals("updated@test.com", rs.getString("email"));
            
            // Second row should be inserted
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("New", rs.getString("name"));
            assertEquals("new@test.com", rs.getString("email"));
            
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGenerateAndExecuteUpsertWithJsonbMerge() throws Exception {
        // Create tables with JSONB columns
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_jsonb_target CASCADE");
            stmt.execute("DROP TABLE IF EXISTS test_jsonb_temp CASCADE");
            
            stmt.execute("CREATE TABLE test_jsonb_target (" +
                    "id INTEGER PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "metadata JSONB" +
                    ")");
            
            stmt.execute("CREATE TABLE test_jsonb_temp (" +
                    "id INTEGER, " +
                    "name VARCHAR(100), " +
                    "metadata JSONB" +
                    ")");
            
            // Insert initial data with JSONB
            stmt.execute("INSERT INTO test_jsonb_target VALUES " +
                    "(1, 'User1', '{\"key1\": \"value1\", \"key2\": \"old\"}')");
            
            // Insert update with partial JSONB update
            stmt.execute("INSERT INTO test_jsonb_temp VALUES " +
                    "(1, 'User1 Updated', '{\"key2\": \"new\", \"key3\": \"added\"}')");
            
            connection.commit();
        }

        // Fetch metadata using the service (tests the production code path with caching)
        TableMetadata metadata = connectionService.getTableMetadata("public", "test_jsonb_target");

        // Verify JSONB column is detected
        assertTrue(metadata.isJsonbColumn("metadata"));

        // Generate upsert SQL with JSONB merge
        String upsertSql = SqlBuilder.buildUpsertStatement(
                metadata, "public", "test_jsonb_target", "test_jsonb_temp", false, false);

        // Verify SQL contains JSONB merge operator
        assertTrue(upsertSql.contains("||"), "Should use JSONB merge operator");

        // Execute the generated SQL
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(upsertSql);
            connection.commit();
        }

        // Verify JSONB values were merged
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, name, metadata::text FROM test_jsonb_target WHERE id = 1")) {
            
            assertTrue(rs.next());
            assertEquals("User1 Updated", rs.getString("name"));
            String jsonbValue = rs.getString("metadata");
            
            // The JSONB should contain merged values
            assertTrue(jsonbValue.contains("key1"), "Should retain key1 from original");
            assertTrue(jsonbValue.contains("key2"), "Should have key2");
            assertTrue(jsonbValue.contains("key3"), "Should have key3 from update");
            assertTrue(jsonbValue.contains("new"), "Should have updated value for key2");
        }
    }

    @Test
    public void testMetadataAndSqlWithChunkedParquetExport() throws Exception {
        // Create table with enough data to trigger chunking
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_chunked_source CASCADE");
            stmt.execute("DROP TABLE IF EXISTS test_chunked_target CASCADE");
            
            stmt.execute("CREATE TABLE test_chunked_source (" +
                    "id SERIAL PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "metadata JSONB" +
                    ")");
            
            stmt.execute("CREATE TABLE test_chunked_target (" +
                    "id INTEGER PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "metadata JSONB" +
                    ")");
            
            // Insert 150 rows to ensure multiple chunks with batch size of 50
            for (int i = 1; i <= 150; i++) {
                stmt.execute(String.format(
                        "INSERT INTO test_chunked_source (name, metadata) VALUES " +
                        "('User%d', '{\"key\": \"value%d\", \"count\": %d}')", i, i, i));
            }
            
            connection.commit();
        }

        // Fetch metadata for source table using the service (tests the production code path with caching)
        TableMetadata metadata = connectionService.getTableMetadata("public", "test_chunked_source");

        // Verify metadata detects JSONB column
        assertTrue(metadata.isJsonbColumn("metadata"), 
                "Should detect metadata as JSONB column");
        assertEquals(3, metadata.getAllColumns().size());

        // Simulate chunked export by reading data in batches
        int batchSize = 50;
        int totalRows = 0;
        int chunkCount = 0;

        try (Statement stmt = connection.createStatement()) {
            // Create temp table for each chunk
            for (int offset = 0; offset < 150; offset += batchSize) {
                chunkCount++;
                String tempTable = "test_chunk_temp_" + chunkCount;
                
                // Drop and create temp table for this chunk
                stmt.execute("DROP TABLE IF EXISTS " + tempTable + " CASCADE");
                stmt.execute("CREATE TABLE " + tempTable + " (" +
                        "id INTEGER, " +
                        "name VARCHAR(100), " +
                        "metadata JSONB" +
                        ")");
                
                // Copy chunk of data to temp table (simulating what would be in a Parquet file)
                int inserted = stmt.executeUpdate(
                        "INSERT INTO " + tempTable + " " +
                        "SELECT id, name, metadata FROM test_chunked_source " +
                        "ORDER BY id LIMIT " + batchSize + " OFFSET " + offset);
                
                if (inserted > 0) {
                    // Generate upsert SQL for this chunk
                    String upsertSql = SqlBuilder.buildUpsertStatement(
                            metadata, "public", "test_chunked_target", tempTable, false, false);

                    // Verify SQL contains JSONB merge
                    assertTrue(upsertSql.contains("||"), 
                            "Chunk " + chunkCount + " SQL should contain JSONB merge operator");

                    // Execute upsert for this chunk
                    stmt.execute(upsertSql);
                    totalRows += inserted;
                    
                    System.out.println("Processed chunk " + chunkCount + " with " + inserted + " rows");
                }
            }
            
            connection.commit();
        }

        // Verify all chunks were processed
        assertTrue(chunkCount >= 3, "Should have processed at least 3 chunks with batch size 50");
        assertEquals(150, totalRows, "Should have processed all 150 rows across chunks");

        // Verify all data was loaded correctly
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_chunked_target")) {
            assertTrue(rs.next());
            assertEquals(150, rs.getInt(1), "Target table should have all 150 rows");
        }

        // Verify JSONB data integrity
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, name, metadata::text FROM test_chunked_target WHERE id IN (25, 75, 125) ORDER BY id")) {
            
            // Check row from first chunk
            assertTrue(rs.next());
            assertEquals(25, rs.getInt("id"));
            assertEquals("User25", rs.getString("name"));
            assertTrue(rs.getString("metadata").contains("value25"));
            
            // Check row from second chunk
            assertTrue(rs.next());
            assertEquals(75, rs.getInt("id"));
            assertEquals("User75", rs.getString("name"));
            assertTrue(rs.getString("metadata").contains("value75"));
            
            // Check row from third chunk
            assertTrue(rs.next());
            assertEquals(125, rs.getInt("id"));
            assertEquals("User125", rs.getString("name"));
            assertTrue(rs.getString("metadata").contains("value125"));
        }
    }
}

