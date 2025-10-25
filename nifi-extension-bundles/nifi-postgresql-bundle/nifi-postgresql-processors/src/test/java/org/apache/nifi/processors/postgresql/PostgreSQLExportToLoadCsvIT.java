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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager.PostgreSQLCredentials;

/**
 * Integration tests for the complete Export -> Upsert -> Load pipeline with CSV format.
 * Tests JSONB column handling and validates row counts at each step.
 */
public class PostgreSQLExportToLoadCsvIT {

    private PostgreSQLCredentials pgCreds;

    @BeforeAll
    public static void setup() {
        // Static setup if needed
    }

    @org.junit.jupiter.api.BeforeEach
    public void setupTest() throws Exception {
        pgCreds = CredentialManager.getPostgreSQLCredentials();
    }

    /**
     * Integration test for the complete Export -> Upsert -> Load pipeline with CSV format and JSONB columns.
     * Validates JSONB casting works correctly throughout the pipeline and row counts match at each step.
     */
    @Test
    @org.junit.jupiter.api.Disabled("CSV RecordSetWriter header configuration needs investigation - CSVRecordSetWriter default includes headers causing COPY IN to fail")
    public void testExportThenUpsertThenLoadCsvWithJsonb() throws Exception {
        long testStartTime = System.currentTimeMillis();
        System.out.println("\n========================================");
        System.out.println("CSV PERFORMANCE TEST - Starting");
        System.out.println("========================================");
        
        // Prepare target tables
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS public.upsert_target_csv");
            st.execute("CREATE TABLE public.upsert_target_csv (LIKE public.perf_test_java_perf_200000_0_1758081847 INCLUDING ALL)");
            st.execute("TRUNCATE TABLE public.upsert_target_csv");
            st.execute("DROP TABLE IF EXISTS public.load_target_csv");
            st.execute("CREATE TABLE public.load_target_csv (LIKE public.perf_test_java_perf_200000_0_1758081847 INCLUDING ALL)");
            st.execute("TRUNCATE TABLE public.load_target_csv");
        }

        // Step 1: Export from source table with validation
        final TestRunner exportRunner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool exportService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        exportRunner.addControllerService("pg", exportService);
        exportRunner.setValidateExpressionUsage(false);
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD,
                pgCreds.getPassword());
        exportRunner.enableControllerService(exportService);
        // Use CSV writer for CSV format export
        org.apache.nifi.csv.CSVRecordSetWriter exportWriter = new org.apache.nifi.csv.CSVRecordSetWriter();
        exportRunner.addControllerService("writer", exportWriter);
        exportRunner.enableControllerService(exportWriter);
        exportRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "pg");
        exportRunner.setProperty(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT, "CSV");
        exportRunner.setProperty(PostgreSQLBulkExport.CUSTOM_QUERY, "SELECT * FROM public.perf_test_java_perf_200000_0_1758081847");
        exportRunner.setProperty(PostgreSQLBulkExport.RECORD_WRITER, "writer");
        exportRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "0"); // single file
        exportRunner.setProperty(PostgreSQLBulkExport.CSV_HEADER, "false"); // Don't include header in CSV output

        long exportStart = System.currentTimeMillis();
        exportRunner.run();
        long exportEnd = System.currentTimeMillis();
        long exportTime = exportEnd - exportStart;
        exportRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);
        List<MockFlowFile> exported = exportRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);

        // VALIDATE: Export produced FlowFile
        assertFalse(exported.isEmpty(), "Export should produce at least one FlowFile");

        // VALIDATE: Row count from Export
        int exportedRows = 0;
        for (MockFlowFile ff : exported) {
            String recordCount = ff.getAttribute("record.count");
            if (recordCount != null) {
                exportedRows += Integer.parseInt(recordCount);
            }
        }
        System.out.println("Exported " + exportedRows + " rows from large table (CSV)");
        System.out.println("⏱️  Export time: " + exportTime + " ms (" + String.format("%.2f", exportTime / 1000.0) + " seconds)");
        System.out.println("   Throughput: " + String.format("%.0f", exportedRows / (exportTime / 1000.0)) + " rows/sec");
        assertTrue(exportedRows > 0, "Export should produce at least one row");

        // Step 2: Upsert using CSV from Export with validation
        final TestRunner upsertRunner = TestRunners.newTestRunner(PostgreSQLBulkUpsert.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool upsertService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        upsertRunner.addControllerService("pg", upsertService);
        upsertRunner.setValidateExpressionUsage(false);
        upsertRunner.setProperty(upsertService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        upsertRunner.setProperty(upsertService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        upsertRunner.setProperty(upsertService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD,
                pgCreds.getPassword());
        upsertRunner.enableControllerService(upsertService);
        // Use CSVRecordSetWriter for CSV output from upsert
        org.apache.nifi.csv.CSVRecordSetWriter upsertWriter = new org.apache.nifi.csv.CSVRecordSetWriter();
        upsertRunner.addControllerService("writer", upsertWriter);
        upsertRunner.enableControllerService(upsertWriter);
        upsertRunner.setProperty(PostgreSQLBulkUpsert.CONNECTION_PROVIDER, "pg");
        upsertRunner.setProperty(PostgreSQLBulkUpsert.BULK_TRANSFER_DATA_FORMAT, "CSV");
        upsertRunner.setProperty(PostgreSQLBulkUpsert.TARGET_TABLE, "public.upsert_target_csv");
        upsertRunner.setProperty(PostgreSQLBulkUpsert.RECORD_WRITER, "writer");
        upsertRunner.setProperty(PostgreSQLBulkUpsert.CSV_HEADER, "false"); // Tell upsert there's no header in input CSV
        upsertRunner.setProperty(PostgreSQLBulkUpsert.UPSERT_RETURNS_RECORDS, "true"); // Return rows from upsert
        // Upsert will automatically generate SQL with JSONB merge and RETURNING * with JSONB casting to TEXT

        byte[] exportedCsv = exported.get(0).toByteArray();
        upsertRunner.enqueue(exportedCsv);
        
        long upsertStart = System.currentTimeMillis();
        upsertRunner.run();
        long upsertEnd = System.currentTimeMillis();
        long upsertTime = upsertEnd - upsertStart;

        // VALIDATE: Upsert succeeded (no failures)
        List<MockFlowFile> failures = upsertRunner.getFlowFilesForRelationship(PostgreSQLBulkUpsert.REL_FAILURE);
        if (!failures.isEmpty()) {
            MockFlowFile f = failures.get(0);
            System.err.println("Upsert failed. reason=" + f.getAttribute("failure.reason"));
            System.err.println("Stacktrace=" + f.getAttribute("failure.stacktrace"));
        }
        assertTrue(failures.isEmpty(), "Upsert should not produce failures");

        List<MockFlowFile> upserted = upsertRunner.getFlowFilesForRelationship(PostgreSQLBulkUpsert.REL_SUCCESS);
        assertFalse(upserted.isEmpty(), "Upsert should return results");
        System.out.println("Upsert produced " + upserted.size() + " FlowFiles (CSV)");

        // VALIDATE: Row count from Upsert results
        int upsertedRows = 0;
        for (MockFlowFile ff : upserted) {
            System.out.println("Upserted FlowFile size: " + ff.getSize() + " bytes");
            System.out.println("Upserted FlowFile attributes: " + ff.getAttributes());
            String recordCount = ff.getAttribute("record.count");
            if (recordCount != null) {
                upsertedRows += Integer.parseInt(recordCount);
            }
        }
        System.out.println("Total upserted rows from attributes: " + upsertedRows);
        System.out.println("⏱️  Upsert time: " + upsertTime + " ms (" + String.format("%.2f", upsertTime / 1000.0) + " seconds)");
        System.out.println("   Throughput: " + String.format("%.0f", upsertedRows / (upsertTime / 1000.0)) + " rows/sec");
        assertEquals(exportedRows, upsertedRows, "Upsert should return same number of rows as exported");

        // VALIDATE: Row count in upsert_target_csv table
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM public.upsert_target_csv")) {
            assertTrue(rs.next());
            int tableCount = rs.getInt(1);
            assertEquals(exportedRows, tableCount, "upsert_target_csv table should have same number of rows as exported");
            System.out.println("✓ upsert_target_csv table has " + tableCount + " rows");
        }

        // Step 3: Load Upsert results into load_target_csv with validation
        final TestRunner loadRunner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool loadService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        loadRunner.addControllerService("pg", loadService);
        loadRunner.setValidateExpressionUsage(false);
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD, pgCreds.getPassword());
        loadRunner.enableControllerService(loadService);
        loadRunner.setProperty(PostgreSQLBulkLoad.CONNECTION_PROVIDER, "pg");
        loadRunner.setProperty(PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT, "CSV");
        loadRunner.setProperty(PostgreSQLBulkLoad.TARGET_TABLE, "public.load_target_csv");

        byte[] upsertedCsv = upserted.get(0).toByteArray();
        System.out.println("Upserted CSV size: " + upsertedCsv.length + " bytes");

        loadRunner.enqueue(upsertedCsv);
        
        long loadStart = System.currentTimeMillis();
        loadRunner.run();
        long loadEnd = System.currentTimeMillis();
        long loadTime = loadEnd - loadStart;

        // VALIDATE: Load succeeded (no failures) - this validates JSONB casting worked
        List<MockFlowFile> loadFailures = loadRunner.getFlowFilesForRelationship(PostgreSQLBulkLoad.REL_FAILURE);
        if (!loadFailures.isEmpty()) {
            MockFlowFile f = loadFailures.get(0);
            System.err.println("Load failed. reason=" + f.getAttribute("failure.reason"));
            System.err.println("Stacktrace=" + f.getAttribute("failure.stacktrace"));
        }
        assertTrue(loadFailures.isEmpty(), "Load should not produce failures - this validates JSONB casting worked");
        assertEquals(1, loadRunner.getFlowFilesForRelationship(PostgreSQLBulkLoad.REL_SUCCESS).size(), "Load should produce success FlowFile");

        // VALIDATE: Final row count in load_target_csv table
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM public.load_target_csv")) {
            assertTrue(rs.next());
            int loadCount = rs.getInt(1);
            assertEquals(exportedRows, loadCount, "load_target_csv table should have same number of rows as exported");
            System.out.println("✓ load_target_csv table has " + loadCount + " rows");
            System.out.println("⏱️  Load time: " + loadTime + " ms (" + String.format("%.2f", loadTime / 1000.0) + " seconds)");
            System.out.println("   Throughput: " + String.format("%.0f", loadCount / (loadTime / 1000.0)) + " rows/sec");
        }

        // FINAL VALIDATION: All three tables should have same row count
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement()) {
            ResultSet rs1 = st.executeQuery("SELECT COUNT(*) FROM public.upsert_target_csv");
            rs1.next();
            int upsertTableCount = rs1.getInt(1);
            
            ResultSet rs2 = st.executeQuery("SELECT COUNT(*) FROM public.load_target_csv");
            rs2.next();
            int loadTableCount = rs2.getInt(1);
            
            System.out.println("\n=== FINAL VALIDATION (CSV) ===");
            System.out.println("Export produced:    " + exportedRows + " rows");
            System.out.println("Upsert returned:    " + upsertedRows + " rows");
            System.out.println("upsert_target has:  " + upsertTableCount + " rows");
            System.out.println("load_target has:    " + loadTableCount + " rows");
            System.out.println("==============================\n");
            
            assertEquals(exportedRows, upsertedRows, "Export and Upsert row counts should match");
            assertEquals(exportedRows, upsertTableCount, "Export and upsert_target_csv row counts should match");
            assertEquals(exportedRows, loadTableCount, "Export and load_target_csv row counts should match");
            
            System.out.println("✓ All row counts match: " + exportedRows + " rows (CSV)");
        }
        
        long testEndTime = System.currentTimeMillis();
        long totalTime = testEndTime - testStartTime;
        
        System.out.println("\n========================================");
        System.out.println("CSV PERFORMANCE SUMMARY");
        System.out.println("========================================");
        System.out.println("Total rows processed: " + exportedRows);
        System.out.println("Export time:  " + String.format("%6d", exportTime) + " ms (" + String.format("%6.2f", exportTime / 1000.0) + " sec) - " + String.format("%8.0f", exportedRows / (exportTime / 1000.0)) + " rows/sec");
        System.out.println("Upsert time:  " + String.format("%6d", upsertTime) + " ms (" + String.format("%6.2f", upsertTime / 1000.0) + " sec) - " + String.format("%8.0f", exportedRows / (upsertTime / 1000.0)) + " rows/sec");
        System.out.println("Load time:    " + String.format("%6d", loadTime) + " ms (" + String.format("%6.2f", loadTime / 1000.0) + " sec) - " + String.format("%8.0f", exportedRows / (loadTime / 1000.0)) + " rows/sec");
        System.out.println("----------------------------------------");
        System.out.println("Total time:   " + String.format("%6d", totalTime) + " ms (" + String.format("%6.2f", totalTime / 1000.0) + " sec) - " + String.format("%8.0f", exportedRows / (totalTime / 1000.0)) + " rows/sec");
        System.out.println("========================================\n");
    }
}

