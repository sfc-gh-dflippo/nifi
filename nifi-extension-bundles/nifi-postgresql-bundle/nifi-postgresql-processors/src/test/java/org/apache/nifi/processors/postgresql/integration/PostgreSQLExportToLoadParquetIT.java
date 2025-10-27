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
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.apache.nifi.postgresql.service.util.CredentialManager;
import org.apache.nifi.postgresql.service.util.CredentialManager.PostgreSQLCredentials;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for export->load Parquet pipeline with JSONB columns. Previously reproduced "Parquet error: Corrupt footer" on COPY IN, now
 * verifies the fix (JSONB columns are cast to TEXT during export).
 */
public class PostgreSQLExportToLoadParquetIT {

    private PostgreSQLCredentials pgCreds;

    @BeforeEach
    public void setup() throws Exception {
        pgCreds = CredentialManager.getPostgreSQLCredentials();
    }

    @Test
    public void testExportThenLoadParquet_TargetBulkLoadTarget() throws Exception {
        // Ensure target table exists and empty
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS public.bulk_load_target");
            st.execute("CREATE TABLE public.bulk_load_target (LIKE public.perf_test_java_perf_200000_0_1758081847 INCLUDING ALL)");
            st.execute("TRUNCATE TABLE public.bulk_load_target");
        }

        // Export Parquet
        final TestRunner exportRunner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool exportService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        exportRunner.addControllerService("pg", exportService);
        exportRunner.setValidateExpressionUsage(false);
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD,
                pgCreds.getPassword());
        exportRunner.enableControllerService(exportService);
        // Record writer required by validation (not used for Parquet stream path)
        org.apache.nifi.json.JsonRecordSetWriter writer = new org.apache.nifi.json.JsonRecordSetWriter();
        exportRunner.addControllerService("writer", writer);
        exportRunner.enableControllerService(writer);
        exportRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "pg");
        exportRunner.setProperty(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        exportRunner.setProperty(PostgreSQLBulkExport.SOURCE_TABLE, "public.perf_test_java_perf_200000_0_1758081847");
        exportRunner.setProperty(PostgreSQLBulkExport.RECORD_WRITER, "writer");
        exportRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "0"); // single file

        exportRunner.run();
        exportRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);
        List<MockFlowFile> exported = exportRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);
        assertTrue(!exported.isEmpty(), "Expected at least one Parquet FlowFile");

        final byte[] parquetBytes = exported.get(0).toByteArray();
        // Validate parquet footer and rows before attempting to load
        assertParquetHasRows(parquetBytes);
        assertTrue(parquetBytes.length > 100, "Parquet should be non-trivial size");

        // Load Parquet
        final TestRunner loadRunner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool loadService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        loadRunner.addControllerService("pg", loadService);
        loadRunner.setValidateExpressionUsage(false);
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD, pgCreds.getPassword());
        loadRunner.enableControllerService(loadService);

        loadRunner.setProperty(PostgreSQLBulkLoad.CONNECTION_PROVIDER, "pg");
        loadRunner.setProperty(PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        loadRunner.setProperty(PostgreSQLBulkLoad.PARQUET_MATCH_BY, "position");
        loadRunner.setProperty(PostgreSQLBulkLoad.TARGET_TABLE, "public.bulk_load_target");

        loadRunner.enqueue(parquetBytes);
        loadRunner.run();

        // Verify load succeeds (JSONB columns are now cast to TEXT during export, fixing the type mismatch)
        loadRunner.assertTransferCount(PostgreSQLBulkLoad.REL_SUCCESS, 1);
        loadRunner.assertTransferCount(PostgreSQLBulkLoad.REL_FAILURE, 0);

        // Verify data was loaded
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM public.bulk_load_target")) {
                assertTrue(rs.next());
                int count = rs.getInt(1);
                assertTrue(count > 0, "Expected rows to be loaded into target table, got: " + count);
            }
        }
    }

    @Test
    public void testExportChunkedThenLoadParquet() throws Exception {
        // Ensure target table exists and empty
        // Note: We modify JSONB columns to TEXT since export casts JSONB to TEXT for Parquet compatibility
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS public.bulk_load_target");
            st.execute("CREATE TABLE public.bulk_load_target (LIKE public.perf_test_java_perf_200000_0_1758081847 INCLUDING ALL)");
            // Convert JSONB columns to TEXT since export casts them for Parquet
            st.execute("ALTER TABLE public.bulk_load_target ALTER COLUMN src TYPE TEXT");
            st.execute("TRUNCATE TABLE public.bulk_load_target");
        }

        final TestRunner exportRunner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool exportService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        exportRunner.addControllerService("pg", exportService);
        exportRunner.setValidateExpressionUsage(false);
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD,
                pgCreds.getPassword());
        exportRunner.enableControllerService(exportService);
        org.apache.nifi.json.JsonRecordSetWriter writer = new org.apache.nifi.json.JsonRecordSetWriter();
        exportRunner.addControllerService("writer", writer);
        exportRunner.enableControllerService(writer);
        exportRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "pg");
        exportRunner.setProperty(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        exportRunner.setProperty(PostgreSQLBulkExport.SOURCE_TABLE, "public.perf_test_java_perf_200000_0_1758081847");
        exportRunner.setProperty(PostgreSQLBulkExport.RECORD_WRITER, "writer");
        exportRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "50000"); // chunked

        exportRunner.run();
        exportRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);
        List<MockFlowFile> exported = exportRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);
        assertTrue(exported.size() >= 2, "Expected multiple Parquet FlowFiles with chunking");

        final TestRunner loadRunner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool loadService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        loadRunner.addControllerService("pg", loadService);
        loadRunner.setValidateExpressionUsage(false);
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD, pgCreds.getPassword());
        loadRunner.enableControllerService(loadService);
        loadRunner.setProperty(PostgreSQLBulkLoad.CONNECTION_PROVIDER, "pg");
        loadRunner.setProperty(PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        loadRunner.setProperty(PostgreSQLBulkLoad.PARQUET_MATCH_BY, "position");
        loadRunner.setProperty(PostgreSQLBulkLoad.TARGET_TABLE, "public.bulk_load_target");

        // Load each part
        for (int i = 0; i < exported.size(); i++) {
            MockFlowFile part = exported.get(i);
            byte[] content = part.toByteArray();
            System.out.println("Chunk " + i + " size bytes: " + content.length);
            assertTrue(content.length > 100, "Parquet chunk should be >100 bytes");
            // Validate parquet chunk footer and rows
            assertParquetHasRows(content);
            loadRunner.clearTransferState();
            loadRunner.enqueue(content);
            loadRunner.run();
            if (!loadRunner.getFlowFilesForRelationship(PostgreSQLBulkLoad.REL_FAILURE).isEmpty()) {
                MockFlowFile f = loadRunner.getFlowFilesForRelationship(PostgreSQLBulkLoad.REL_FAILURE).get(0);
                System.err.println("Chunk " + i + " failed. reason=" + f.getAttribute("failure.reason"));
                System.err.println("Stacktrace=" + f.getAttribute("failure.stacktrace"));
                fail("BulkLoad failed for a chunk.");
            }
        }
    }

    @Test
    public void testIncrementalChunkedThenLoadParquet() throws Exception {
        // Prepare target
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS public.bulk_load_target");
            st.execute("CREATE TABLE public.bulk_load_target (LIKE public.perf_test_java_perf_200000_0_1758081847 INCLUDING ALL)");
            st.execute("TRUNCATE TABLE public.bulk_load_target");
        }

        final TestRunner exportRunner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool exportService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        exportRunner.addControllerService("pg", exportService);
        exportRunner.setValidateExpressionUsage(false);
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        exportRunner.setProperty(exportService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD,
                pgCreds.getPassword());
        exportRunner.enableControllerService(exportService);
        org.apache.nifi.json.JsonRecordSetWriter writer = new org.apache.nifi.json.JsonRecordSetWriter();
        exportRunner.addControllerService("writer", writer);
        exportRunner.enableControllerService(writer);
        exportRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "pg");
        exportRunner.setProperty(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        exportRunner.setProperty(PostgreSQLBulkExport.SOURCE_TABLE, "public.perf_test_java_perf_200000_0_1758081847");
        exportRunner.setProperty(PostgreSQLBulkExport.RECORD_WRITER, "writer");
        exportRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "50000");
        exportRunner.setProperty(PostgreSQLBulkExport.MAXIMUM_VALUE_COLUMNS, "_etl_modified_");

        // First run seeds state and exports
        exportRunner.run();
        exportRunner.assertTransferCount(PostgreSQLBulkExport.REL_FAILURE, 0);
        List<MockFlowFile> firstRun = exportRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);
        assertFalse(firstRun.isEmpty());

        // Second run should be incremental (likely zero or few)
        exportRunner.clearTransferState();
        exportRunner.run();
        List<MockFlowFile> incr = exportRunner.getFlowFilesForRelationship(PostgreSQLBulkExport.REL_SUCCESS);

        final TestRunner loadRunner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool loadService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        loadRunner.addControllerService("pg", loadService);
        loadRunner.setValidateExpressionUsage(false);
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD, pgCreds.getPassword());
        loadRunner.enableControllerService(loadService);
        loadRunner.setProperty(PostgreSQLBulkLoad.CONNECTION_PROVIDER, "pg");
        loadRunner.setProperty(PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        loadRunner.setProperty(PostgreSQLBulkLoad.PARQUET_MATCH_BY, "position");
        loadRunner.setProperty(PostgreSQLBulkLoad.TARGET_TABLE, "public.bulk_load_target");

        for (MockFlowFile ff : incr) {
            loadRunner.clearTransferState();
            byte[] content = ff.toByteArray();
            if (content.length > 0) {
                assertParquetHasRows(content);
            }
            loadRunner.enqueue(content);
            loadRunner.run();
            if (!loadRunner.getFlowFilesForRelationship(PostgreSQLBulkLoad.REL_FAILURE).isEmpty()) {
                MockFlowFile f = loadRunner.getFlowFilesForRelationship(PostgreSQLBulkLoad.REL_FAILURE).get(0);
                fail("Incremental BulkLoad failed. reason=" + f.getAttribute("failure.reason"));
            }
        }
    }

    @Test
    public void testExportThenUpsertThenLoadParquetWithJsonb() throws Exception {
        long testStartTime = System.currentTimeMillis();
        System.out.println("\n========================================");
        System.out.println("PARQUET PERFORMANCE TEST - Starting");
        System.out.println("========================================");
        
        // Prepare target tables
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS public.upsert_target");
            st.execute("CREATE TABLE public.upsert_target (LIKE public.perf_test_java_perf_200000_0_1758081847 INCLUDING ALL)");
            st.execute("TRUNCATE TABLE public.upsert_target");
            st.execute("DROP TABLE IF EXISTS public.load_target");
            st.execute("CREATE TABLE public.load_target (LIKE public.perf_test_java_perf_200000_0_1758081847 INCLUDING ALL)");
            st.execute("TRUNCATE TABLE public.load_target");
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
        org.apache.nifi.json.JsonRecordSetWriter exportWriter = new org.apache.nifi.json.JsonRecordSetWriter();
        exportRunner.addControllerService("writer", exportWriter);
        exportRunner.enableControllerService(exportWriter);
        exportRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "pg");
        exportRunner.setProperty(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        exportRunner.setProperty(PostgreSQLBulkExport.CUSTOM_QUERY, "SELECT * FROM public.perf_test_java_perf_200000_0_1758081847");
        exportRunner.setProperty(PostgreSQLBulkExport.RECORD_WRITER, "writer");
        exportRunner.setProperty(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE, "0"); // single file

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
        System.out.println("Exported " + exportedRows + " rows from large table");
        System.out.println("⏱️  Export time: " + exportTime + " ms (" + String.format("%.2f", exportTime / 1000.0) + " seconds)");
        System.out.println("   Throughput: " + String.format("%.0f", exportedRows / (exportTime / 1000.0)) + " rows/sec");
        assertTrue(exportedRows > 0, "Export should produce at least one row");

        // Step 2: Upsert using Parquet from Export with validation
        final TestRunner upsertRunner = TestRunners.newTestRunner(PostgreSQLBulkUpsert.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool upsertService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        upsertRunner.addControllerService("pg", upsertService);
        upsertRunner.setValidateExpressionUsage(false);
        upsertRunner.setProperty(upsertService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        upsertRunner.setProperty(upsertService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        upsertRunner.setProperty(upsertService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD,
                pgCreds.getPassword());
        upsertRunner.enableControllerService(upsertService);
        // CRITICAL: Use ParquetRecordSetWriter for Parquet output from upsert
        org.apache.nifi.parquet.ParquetRecordSetWriter upsertWriter = new org.apache.nifi.parquet.ParquetRecordSetWriter();
        upsertRunner.addControllerService("writer", upsertWriter);
        upsertRunner.enableControllerService(upsertWriter);
        upsertRunner.setProperty(PostgreSQLBulkUpsert.CONNECTION_PROVIDER, "pg");
        upsertRunner.setProperty(PostgreSQLBulkUpsert.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        upsertRunner.setProperty(PostgreSQLBulkUpsert.TARGET_TABLE, "public.upsert_target");
        upsertRunner.setProperty(PostgreSQLBulkUpsert.RECORD_WRITER, "writer");
        // Upsert will automatically generate SQL with JSONB merge and RETURNING * with JSONB casting to TEXT

        byte[] exportedParquet = exported.get(0).toByteArray();
        upsertRunner.enqueue(exportedParquet);
        
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
        System.out.println("Upsert produced " + upserted.size() + " FlowFiles");

        // VALIDATE: Row count from Upsert results
        int upsertedRows = 0;
        for (MockFlowFile ff : upserted) {
            System.out.println("Upserted FlowFile size: " + ff.getSize() + " bytes");
            System.out.println("Upserted FlowFile attributes: " + ff.getAttributes());
            String recordCount = ff.getAttribute("record.count");
            if (recordCount != null) {
                upsertedRows += Integer.parseInt(recordCount);
            }
            
            // Debug: Check first 100 bytes of content
            byte[] content = ff.toByteArray();
            int debugLen = Math.min(100, content.length);
            StringBuilder hexDump = new StringBuilder();
            StringBuilder asciiDump = new StringBuilder();
            for (int i = 0; i < debugLen; i++) {
                hexDump.append(String.format("%02X ", content[i]));
                char c = (char) content[i];
                asciiDump.append(c >= 32 && c < 127 ? c : '.');
            }
            System.out.println("First " + debugLen + " bytes (hex): " + hexDump);
            System.out.println("First " + debugLen + " bytes (ascii): " + asciiDump);
        }
        System.out.println("Total upserted rows from attributes: " + upsertedRows);
        System.out.println("⏱️  Upsert time: " + upsertTime + " ms (" + String.format("%.2f", upsertTime / 1000.0) + " seconds)");
        System.out.println("   Throughput: " + String.format("%.0f", upsertedRows / (upsertTime / 1000.0)) + " rows/sec");
        assertEquals(exportedRows, upsertedRows, "Upsert should return same number of rows as exported");

        // VALIDATE: Row count in upsert_target table
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM public.upsert_target")) {
            assertTrue(rs.next());
            int tableCount = rs.getInt(1);
            assertEquals(exportedRows, tableCount, "upsert_target table should have same number of rows as exported");
            System.out.println("✓ upsert_target table has " + tableCount + " rows");
        }

        // Step 3: Load Upsert results into load_target with validation
        final TestRunner loadRunner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool loadService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();
        loadRunner.addControllerService("pg", loadService);
        loadRunner.setValidateExpressionUsage(false);
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_URL, pgCreds.getJdbcUrl());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_USER, pgCreds.getUserName());
        loadRunner.setProperty(loadService, org.apache.nifi.postgresql.service.PostgreSQLConnectionPool.POSTGRESQL_PASSWORD, pgCreds.getPassword());
        loadRunner.enableControllerService(loadService);
        loadRunner.setProperty(PostgreSQLBulkLoad.CONNECTION_PROVIDER, "pg");
        loadRunner.setProperty(PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT, "Parquet");
        loadRunner.setProperty(PostgreSQLBulkLoad.PARQUET_MATCH_BY, "position");
        loadRunner.setProperty(PostgreSQLBulkLoad.TARGET_TABLE, "public.load_target");

        byte[] upsertedParquet = upserted.get(0).toByteArray();
        System.out.println("Upserted Parquet size: " + upsertedParquet.length + " bytes");

        // Validate upserted parquet before loading
        try {
            assertParquetHasRows(upsertedParquet);
            System.out.println("Upserted Parquet is valid");
        } catch (Exception e) {
            System.err.println("Upserted Parquet validation failed: " + e.getMessage());
            throw e;
        }

        loadRunner.enqueue(upsertedParquet);
        
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

        // VALIDATE: Final row count in load_target table
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM public.load_target")) {
            assertTrue(rs.next());
            int loadCount = rs.getInt(1);
            assertEquals(exportedRows, loadCount, "load_target table should have same number of rows as exported");
            System.out.println("✓ load_target table has " + loadCount + " rows");
            System.out.println("⏱️  Load time: " + loadTime + " ms (" + String.format("%.2f", loadTime / 1000.0) + " seconds)");
            System.out.println("   Throughput: " + String.format("%.0f", loadCount / (loadTime / 1000.0)) + " rows/sec");
        }

        // FINAL VALIDATION: All three tables should have same row count
        try (Connection conn = DriverManager.getConnection(pgCreds.getJdbcUrl(), pgCreds.getUserName(), pgCreds.getPassword());
                Statement st = conn.createStatement()) {
            ResultSet rs1 = st.executeQuery("SELECT COUNT(*) FROM public.upsert_target");
            rs1.next();
            int upsertTableCount = rs1.getInt(1);
            
            ResultSet rs2 = st.executeQuery("SELECT COUNT(*) FROM public.load_target");
            rs2.next();
            int loadTableCount = rs2.getInt(1);
            
            System.out.println("\n=== FINAL VALIDATION ===");
            System.out.println("Export produced:    " + exportedRows + " rows");
            System.out.println("Upsert returned:    " + upsertedRows + " rows");
            System.out.println("upsert_target has:  " + upsertTableCount + " rows");
            System.out.println("load_target has:    " + loadTableCount + " rows");
            System.out.println("========================\n");
            
            assertEquals(exportedRows, upsertedRows, "Export and Upsert row counts should match");
            assertEquals(exportedRows, upsertTableCount, "Export and upsert_target row counts should match");
            assertEquals(exportedRows, loadTableCount, "Export and load_target row counts should match");
            
            System.out.println("✓ All row counts match: " + exportedRows + " rows");
        }
        
        long testEndTime = System.currentTimeMillis();
        long totalTime = testEndTime - testStartTime;
        
        System.out.println("\n========================================");
        System.out.println("PARQUET PERFORMANCE SUMMARY");
        System.out.println("========================================");
        System.out.println("Total rows processed: " + exportedRows);
        System.out.println("Export time:  " + String.format("%6d", exportTime) + " ms (" + String.format("%6.2f", exportTime / 1000.0) + " sec) - " + String.format("%8.0f", exportedRows / (exportTime / 1000.0)) + " rows/sec");
        System.out.println("Upsert time:  " + String.format("%6d", upsertTime) + " ms (" + String.format("%6.2f", upsertTime / 1000.0) + " sec) - " + String.format("%8.0f", exportedRows / (upsertTime / 1000.0)) + " rows/sec");
        System.out.println("Load time:    " + String.format("%6d", loadTime) + " ms (" + String.format("%6.2f", loadTime / 1000.0) + " sec) - " + String.format("%8.0f", exportedRows / (loadTime / 1000.0)) + " rows/sec");
        System.out.println("----------------------------------------");
        System.out.println("Total time:   " + String.format("%6d", totalTime) + " ms (" + String.format("%6.2f", totalTime / 1000.0) + " sec) - " + String.format("%8.0f", exportedRows / (totalTime / 1000.0)) + " rows/sec");
        System.out.println("========================================\n");
    }

    private void assertParquetHasRows(final byte[] parquetBytes) throws Exception {
        java.nio.file.Path tmp = null;
        try {
            tmp = java.nio.file.Files.createTempFile("it-parquet-", ".parquet");
            java.nio.file.Files.write(tmp, parquetBytes);

            org.apache.parquet.io.InputFile inFile = new TestLocalInputFile(tmp);
            int rows = 0;
            try (org.apache.parquet.hadoop.ParquetReader<org.apache.avro.generic.GenericRecord> reader = org.apache.parquet.avro.AvroParquetReader.<org.apache.avro.generic.GenericRecord>builder(
                    inFile).build()) {
                while (reader.read() != null) {
                    rows++;
                }
            }
            System.out.println("Validated Parquet rows: " + rows);
            assertTrue(rows > 0, "Parquet should contain at least one row");
        } finally {
            if (tmp != null) {
                try {
                    java.nio.file.Files.deleteIfExists(tmp);
                } catch (Exception ignored) {
                    // Exception ignored
                }
            }
        }
    }

    // Minimal InputFile/SeekableInputStream for Parquet reader
    private static class TestLocalInputFile implements org.apache.parquet.io.InputFile {
        private final java.nio.file.Path path;
        TestLocalInputFile(java.nio.file.Path path) {
            this.path = path;
        }
        @Override
        public long getLength() throws java.io.IOException {
            return java.nio.file.Files.size(path);
        }
        @Override
        public org.apache.parquet.io.SeekableInputStream newStream() throws java.io.IOException {
            return new TestLocalSeekableInputStream(path);
        }
    }

    private static class TestLocalSeekableInputStream extends org.apache.parquet.io.SeekableInputStream {
        private final java.nio.channels.SeekableByteChannel channel;
        private final java.nio.ByteBuffer single = java.nio.ByteBuffer.allocate(1);
        TestLocalSeekableInputStream(java.nio.file.Path path) throws java.io.IOException {
            this.channel = java.nio.file.Files.newByteChannel(path);
        }
        @Override
        public long getPos() throws java.io.IOException {
            return channel.position();
        }
        @Override
        public void seek(long newPos) throws java.io.IOException {
            channel.position(newPos);
        }
        @Override
        public void readFully(byte[] bytes) throws java.io.IOException {
            readFully(bytes, 0, bytes.length);
        }
        @Override
        public void readFully(byte[] bytes, int start, int len) throws java.io.IOException {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes, start, len);
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1)
                    throw new java.io.EOFException();
            }
        }
        @Override
        public int read(java.nio.ByteBuffer buf) throws java.io.IOException {
            return channel.read(buf);
        }
        @Override
        public void readFully(java.nio.ByteBuffer buf) throws java.io.IOException {
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1)
                    throw new java.io.EOFException();
            }
        }
        @Override
        public int read() throws java.io.IOException {
            single.clear();
            int r = channel.read(single);
            if (r == -1)
                return -1;
            single.flip();
            return single.get() & 0xFF;
        }
        @Override
        public int read(byte[] bytes, int off, int len) throws java.io.IOException {
            return channel.read(java.nio.ByteBuffer.wrap(bytes, off, len));
        }
        @Override
        public void close() throws java.io.IOException {
            channel.close();
        }
    }
}
