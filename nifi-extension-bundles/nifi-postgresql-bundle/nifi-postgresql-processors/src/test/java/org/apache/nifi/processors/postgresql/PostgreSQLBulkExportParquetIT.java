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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager.PostgreSQLCredentials;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for PostgreSQLBulkExport with Parquet format against a remote PostgreSQL database. Uses CredentialManager to read connection
 * details from ~/.pg_service.conf
 *
 * Tests against table: public.perf_test_java_perf_200000_0_1758081847 (110,000 rows)
 */
public class PostgreSQLBulkExportParquetIT {

    private TestRunner testRunner;
    private PostgreSQLCredentials pgCreds;
    private org.apache.nifi.postgresql.service.PostgreSQLConnectionPool connectionService;

    @BeforeEach
    public void setup() throws Exception {
        // Get PostgreSQL credentials from ~/.pg_service.conf
        pgCreds = CredentialManager.getPostgreSQLCredentials();

        System.out.println("=== PostgreSQL Connection Info ===");
        System.out.println("Host: " + pgCreds.getHost());
        System.out.println("Port: " + pgCreds.getPort());
        System.out.println("Database: " + pgCreds.getDatabase());
        System.out.println("Username: " + pgCreds.getUserName());
        System.out.println("JDBC URL: " + pgCreds.getJdbcUrl());
        System.out.println("===================================");

        // Initialize the processor and test runner
        testRunner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        testRunner.setValidateExpressionUsage(false);
        // Clear state for repeatable test cycles
        testRunner.getStateManager().clear(org.apache.nifi.components.state.Scope.CLUSTER);

        // Create and configure the PostgreSQL connection pool service
        connectionService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        testRunner.addControllerService("postgresql-service", connectionService);

        testRunner.setProperty(connectionService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        testRunner.setProperty(connectionService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        testRunner.setProperty(connectionService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD,
                pgCreds.getPassword());

        // Enable the connection service
        testRunner.assertValid(connectionService);
        testRunner.enableControllerService(connectionService);

        // Add a simple RecordWriter service for output
        // Note: Even though we're using Parquet format, the processor transcodes
        // through ParquetExportStreamer
        org.apache.nifi.json.JsonRecordSetWriter writerService = new org.apache.nifi.json.JsonRecordSetWriter();
        testRunner.addControllerService("record-writer", writerService);
        testRunner.enableControllerService(writerService);

        // Configure the processor
        testRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "postgresql-service");
        testRunner.setProperty(PostgreSQLBulkExport.SOURCE_TABLE, "public.perf_test_java_perf_200000_0_1758081847");
        testRunner.setProperty(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        testRunner.setProperty(PostgreSQLBulkExport.RECORD_WRITER, "record-writer");
        testRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "100000"); // Enable chunking
    }

    /**
     * Test that verifies the table exists and has data
     */
    @Test
    public void testTableExistsAndHasData() throws Exception {
        String jdbcUrl = pgCreds.getJdbcUrl();
        String username = pgCreds.getUserName();
        String password = pgCreds.getPassword();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as row_count FROM public.perf_test_java_perf_200000_0_1758081847")) {

            assertTrue(rs.next(), "Query should return a result");
            long rowCount = rs.getLong("row_count");

            System.out.println("Table row count: " + rowCount);
            assertTrue(rowCount > 0, "Table should have rows");
            assertTrue(rowCount >= 110000, "Table should have at least 110,000 rows");
        }
    }

    /**
     * Test that verifies pg_parquet extension is available
     */
    @Test
    public void testPgParquetExtensionAvailable() throws Exception {
        String jdbcUrl = pgCreds.getJdbcUrl();
        String username = pgCreds.getUserName();
        String password = pgCreds.getPassword();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {

            // Try to use pg_parquet COPY TO
            String query = "SELECT * FROM public.perf_test_java_perf_200000_0_1758081847 LIMIT 1";
            String copyQuery = String.format("COPY (%s) TO STDOUT (FORMAT PARQUET)", query);

            // This will throw an exception if pg_parquet is not available
            org.postgresql.copy.CopyManager copyManager = new org.postgresql.copy.CopyManager((org.postgresql.core.BaseConnection) conn);

            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            copyManager.copyOut(copyQuery, out);

            byte[] parquetData = out.toByteArray();
            System.out.println("Parquet data size from 1 row: " + parquetData.length + " bytes");

            assertTrue(parquetData.length > 0, "Parquet data should not be empty");
        }
    }

    /**
     * Test bulk export with Parquet format - no chunking (all rows in one FlowFile)
     */
    @Test
    public void testBulkExportParquetNoChunking() throws Exception {
        // Disable chunking to get all rows in one FlowFile
        testRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "0");

        // Run the processor
        testRunner.run();

        // Verify results
        testRunner.assertTransferCount(PostgreSQLBulkExport.REL_SUCCESS, 1);
        testRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);
        MockFlowFile result = results.get(0);

        // Verify FlowFile attributes
        System.out.println("FlowFile size: " + result.getSize());
        System.out.println("FlowFile attributes: " + result.getAttributes());

        assertTrue(result.getSize() > 0, "FlowFile should not be empty");

        String recordCount = result.getAttribute("record.count");
        assertNotNull(recordCount, "record.count attribute should be set");
        int records = Integer.parseInt(recordCount);
        assertTrue(records >= 110000, "Should have exported at least 110,000 records");

        System.out.println("Successfully exported " + records + " records in Parquet format");
    }

    /**
     * Test bulk export with Parquet format - with chunking (multiple FlowFiles)
     */
    @Test
    public void testBulkExportParquetWithChunking() throws Exception {
        // Set chunk size to 50,000 rows - should result in at least 3 FlowFiles
        testRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "50000");

        // Run the processor
        testRunner.run();

        // Verify results
        testRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);

        System.out.println("Number of FlowFiles created: " + results.size());
        assertTrue(results.size() >= 2, "Should have created at least 2 FlowFiles for 110,000 rows with 50,000 row chunks");

        int totalRecords = 0;
        for (int i = 0; i < results.size(); i++) {
            MockFlowFile flowFile = results.get(i);
            System.out.println("FlowFile " + (i + 1) + " size: " + flowFile.getSize() + " bytes");

            assertTrue(flowFile.getSize() > 0, "FlowFile " + (i + 1) + " should not be empty");

            String recordCount = flowFile.getAttribute("record.count");
            assertNotNull(recordCount, "FlowFile " + (i + 1) + " should have record.count attribute");

            int records = Integer.parseInt(recordCount);
            System.out.println("FlowFile " + (i + 1) + " records: " + records);

            totalRecords += records;
        }

        System.out.println("Total records exported: " + totalRecords);
        assertTrue(totalRecords >= 110000, "Should have exported at least 110,000 total records");
    }

    /**
     * Test that verifies the actual Parquet bytes are being produced (not zero-length)
     */
    @Test
    public void testParquetBytesAreNotZero() throws Exception {
        // Use small chunk to make test faster
        testRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "1000");

        // Run the processor
        testRunner.run();

        // Check for failures first and print the error
        List<MockFlowFile> failures = testRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_FAILURE);
        if (!failures.isEmpty()) {
            System.err.println("=== PROCESSOR FAILED ===");
            for (MockFlowFile failure : failures) {
                System.err.println("Failure reason: " + failure.getAttribute("failure.reason"));
                System.err.println("Failure stacktrace: " + failure.getAttribute("failure.stacktrace"));
                System.err.println("All attributes: " + failure.getAttributes());
            }
            fail("Processor routed to FAILURE. This reproduces the bug we're trying to fix!");
        }

        // testRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);
        assertTrue(!results.isEmpty(), "Should have created at least one FlowFile");

        // Check each FlowFile
        for (MockFlowFile flowFile : results) {
            long size = flowFile.getSize();
            System.out.println("FlowFile content size: " + size + " bytes");

            assertNotEquals(0, size, "FlowFile should not be zero bytes (this was the bug we're testing for)");
            assertTrue(size > 100, "Parquet file should have substantial data (headers + data)");

            // Verify it's actual content, not just whitespace
            byte[] content = flowFile.toByteArray();
            assertNotNull(content, "FlowFile content should not be null");
            assertEquals(size, content.length, "Content length should match reported size");
        }
    }

    /**
     * Test Maximum-value Columns feature with _etl_modified_ column This tests incremental fetching using a numeric timestamp column
     */
    @Test
    public void testMaximumValueColumnsWithEtlModified() throws Exception {
        // Clear any existing state
        testRunner.getStateManager().clear(org.apache.nifi.components.state.Scope.CLUSTER);

        // Configure Maximum-value Columns
        testRunner.setProperty(PostgreSQLBulkExport.MAXIMUM_VALUE_COLUMNS, "_etl_modified_");
        testRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "50000"); // Enable chunking

        System.out.println("\n=== First Run: Should fetch all records ===");

        // First run - should fetch all records
        testRunner.run();

        testRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);

        List<MockFlowFile> firstRun = testRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);
        assertTrue(!firstRun.isEmpty(), "Should have created at least one FlowFile");

        int totalRecordsFirstRun = 0;
        for (MockFlowFile flowFile : firstRun) {
            String recordCount = flowFile.getAttribute("record.count");
            if (recordCount != null) {
                totalRecordsFirstRun += Integer.parseInt(recordCount);
            }
        }

        System.out.println("First run exported " + totalRecordsFirstRun + " records");
        assertTrue(totalRecordsFirstRun >= 110000, "First run should have exported at least 110,000 records");

        // Verify state was stored
        org.apache.nifi.components.state.StateMap state = testRunner.getStateManager().getState(org.apache.nifi.components.state.Scope.CLUSTER);
        assertNotNull(state, "State should have been saved");

        String storedMaxValue = null;
        for (java.util.Map.Entry<String, String> e : state.toMap().entrySet()) {
            if (e.getKey().endsWith("._etl_modified_")) {
                storedMaxValue = e.getValue();
                break;
            }
        }
        assertNotNull(storedMaxValue, "Maximum value for _etl_modified_ should be stored in state");

        System.out.println("Stored maximum _etl_modified_ value: " + storedMaxValue);

        // Verify the stored value is numeric (not quoted)
        assertTrue(storedMaxValue.matches("\\d+"), "Stored value should be numeric: " + storedMaxValue);

        long maxValue = Long.parseLong(storedMaxValue);
        assertTrue(maxValue > 0, "Maximum value should be greater than 0");

        System.out.println("\n=== Second Run: Should fetch only new records (none expected) ===");

        // Clear the output from first run
        testRunner.clearTransferState();

        // Second run - should fetch NO records (since no new data)
        testRunner.run();

        List<MockFlowFile> secondRun = testRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);

        int totalRecordsSecondRun = 0;
        for (MockFlowFile flowFile : secondRun) {
            String recordCount = flowFile.getAttribute("record.count");
            if (recordCount != null) {
                totalRecordsSecondRun += Integer.parseInt(recordCount);
            }
        }

        System.out.println("Second run exported " + totalRecordsSecondRun + " records");

        // After initial max-value write, a second run should produce 0 unless data
        // changed
        assertTrue(totalRecordsSecondRun == 0, "Second run should export zero records after state set");

        // Now re-run with initial.maxvalue set to one day lower than stored max to
        // reprocess only latest records
        // Clear state so initial.maxvalue is used
        testRunner.getStateManager().clear(org.apache.nifi.components.state.Scope.CLUSTER);

        // Compute one-day-earlier ISO timestamp
        // storedMaxValue is in microseconds since epoch, convert to Instant
        long maxMicros = Long.parseLong(storedMaxValue);
        java.time.Instant maxInstant = java.time.Instant.ofEpochSecond(maxMicros / 1_000_000, (maxMicros % 1_000_000) * 1000);
        java.time.Instant oneDayEarlier = maxInstant.minus(java.time.Duration.ofDays(1));
        String seedIso = java.time.format.DateTimeFormatter.ISO_INSTANT.format(oneDayEarlier);

        // Set dynamic property to seed initial max
        testRunner.setProperty("initial.maxvalue._etl_modified_", seedIso);

        // Third run - should fetch only the latest records after the seed time
        testRunner.clearTransferState();
        testRunner.run();
        List<MockFlowFile> thirdRun = testRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);
        int totalRecordsThirdRun = 0;
        for (MockFlowFile flowFile : thirdRun) {
            String recordCount = flowFile.getAttribute("record.count");
            if (recordCount != null)
                totalRecordsThirdRun += Integer.parseInt(recordCount);
        }
        System.out.println("Third run (seeded) exported " + totalRecordsThirdRun + " records");
        assertTrue(totalRecordsThirdRun > 0, "Seeded third run should export latest records");

        System.out.println("\n=== Test Summary ===");
        System.out.println("✅ Maximum-value Columns feature works correctly");
        System.out.println("✅ Numeric timestamp values handled properly (no quoting)");
        System.out.println("✅ Incremental fetching works as expected");
    }

    /**
     * Test Maximum-value Columns with custom query
     */
    @Test
    public void testMaximumValueColumnsWithCustomQuery() throws Exception {
        // Clear any existing state
        testRunner.getStateManager().clear(org.apache.nifi.components.state.Scope.CLUSTER);

        // Remove SOURCE_TABLE and set CUSTOM_QUERY
        testRunner.removeProperty(PostgreSQLBulkExport.SOURCE_TABLE);
        testRunner.setProperty(PostgreSQLBulkExport.CUSTOM_QUERY,
                "SELECT * FROM public.perf_test_java_perf_200000_0_1758081847 WHERE _etl_modified_ > '1970-01-01T00:00:00Z'");
        testRunner.setProperty(PostgreSQLBulkExport.MAXIMUM_VALUE_COLUMNS, "_etl_modified_");
        testRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "0"); // No chunking

        System.out.println("\n=== Testing Maximum-value with Custom Query ===");

        // Run the processor
        testRunner.run();

        testRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);
        assertTrue(!results.isEmpty(), "Should have created at least one FlowFile");

        MockFlowFile result = results.get(0);
        String recordCount = result.getAttribute("record.count");
        assertNotNull(recordCount, "record.count attribute should be set");

        int records = Integer.parseInt(recordCount);
        System.out.println("Exported " + records + " records with custom query");
        assertTrue(records > 0, "Should have exported records");

        // Verify state was stored
        org.apache.nifi.components.state.StateMap state = testRunner.getStateManager().getState(org.apache.nifi.components.state.Scope.CLUSTER);
        String storedMaxValue = null;
        for (java.util.Map.Entry<String, String> e : state.toMap().entrySet()) {
            if (e.getKey().endsWith("._etl_modified_")) {
                storedMaxValue = e.getValue();
                break;
            }
        }
        assertNotNull(storedMaxValue, "Maximum value should be stored");
        System.out.println("✅ Maximum-value Columns works with custom queries. Stored max: " + storedMaxValue);
    }
}
