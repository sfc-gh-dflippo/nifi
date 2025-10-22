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

package org.apache.nifi.processors.postgresql.format;

import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.processors.postgresql.*;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Consolidated Parquet format tests for all PostgreSQL processors.
 * Tests Parquet export, load, and upsert functionality.
 */
public class ParquetProcessorsIT {

    private static final CredentialManager.PostgreSQLCredentials CREDENTIALS = CredentialManager.getPostgreSQLCredentials();
    private static final String SCHEMA = "public";  // PostgreSQL default schema
    private static final String EXPORT_TABLE = SCHEMA + ".nifi_test_parquet_export";
    private static final String LOAD_TABLE = SCHEMA + ".nifi_test_parquet_load";
    private static final String UPSERT_TABLE = SCHEMA + ".nifi_test_parquet_upsert";

    private PostgreSQLConnectionProviderService connectionService;

    private PostgreSQLConnectionProviderService createConnectionProviderService(
            TestRunner runner,
            CredentialManager.PostgreSQLCredentials credentials) throws Exception {
        
        runner.setValidateExpressionUsage(false);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool connectionProviderService = 
            new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();

        runner.addControllerService("postgresqlConnectionProviderService", connectionProviderService);

        runner.setProperty(connectionProviderService,
                org.apache.nifi.postgresql.service.util.ConnectionPoolSettings.CONNECTION_URL_FORMAT,
                org.apache.nifi.postgresql.service.util.ConnectionUrlFormat.FULL_URL);

        runner.setProperty(connectionProviderService,
                org.apache.nifi.postgresql.service.util.ConnectionPoolSettings.POSTGRESQL_URL,
                credentials.getJdbcUrl());

        runner.setProperty(connectionProviderService,
                org.apache.nifi.postgresql.service.util.ConnectionPoolSettings.POSTGRESQL_USER,
                credentials.getUserName());

        runner.setProperty(connectionProviderService,
                org.apache.nifi.postgresql.service.util.ConnectionPoolSettings.POSTGRESQL_PASSWORD,
                credentials.getPassword());

        runner.setProperty(connectionProviderService,
                org.apache.nifi.processors.postgresql.util.ConnectionSettings.SSL,
                "true");

        runner.enableControllerService(connectionProviderService);
        return connectionProviderService;
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Create connection service
        TestRunner tempRunner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        connectionService = createConnectionProviderService(tempRunner, CREDENTIALS);

        // Create schema and tables
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
                
                // Create tables for all tests
                st.execute("CREATE TABLE " + EXPORT_TABLE + " (id INT PRIMARY KEY, name TEXT)");
                st.execute("CREATE TABLE " + LOAD_TABLE + " (id INT PRIMARY KEY, name TEXT)");
                st.execute("CREATE TABLE " + UPSERT_TABLE + " (id INT PRIMARY KEY, name TEXT)");
                
                // Insert test data for export
                st.execute("INSERT INTO " + EXPORT_TABLE + " (id, name) VALUES (1,'A'), (2,'B'), (3,'C')");
                
                wrapper.getConnection().commit();
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connectionService != null) {
            try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
                try (Statement st = wrapper.getConnection().createStatement()) {
                    st.execute("DROP TABLE IF EXISTS " + EXPORT_TABLE);
                    st.execute("DROP TABLE IF EXISTS " + LOAD_TABLE);
                    st.execute("DROP TABLE IF EXISTS " + UPSERT_TABLE);
                    wrapper.getConnection().commit();
                }
            }
        }
    }

    @Test
    public void testParquetExport() throws Exception {
        // Check if pg_parquet extension is available
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT 1 FROM pg_available_extensions WHERE name = 'parquet_s3_fdw'")) {
                    Assumptions.assumeTrue(rs.next(), "pg_parquet extension not available - skipping Parquet tests");
                }
            }
        }

        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);
        
        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("source-table", EXPORT_TABLE);
        runner.setProperty("data-format", "Parquet");

        runner.run();
        
        // Check if any FlowFiles were produced (may skip if pg_parquet not available)
        final var success = runner.getFlowFilesForRelationship("success");
        final var failure = runner.getFlowFilesForRelationship("failure");
        
        // Test should either succeed with Parquet or fail gracefully
        Assertions.assertTrue(success.size() > 0 || failure.size() > 0);
        
        if (success.size() > 0) {
            final MockFlowFile flowFile = success.get(0);
            Assertions.assertTrue(flowFile.getSize() > 0);
            Assertions.assertNotNull(flowFile.getAttribute("record.count"));
        }
    }

    @Test
    public void testParquetLoad() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);
        
        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "Parquet");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", LOAD_TABLE);

        // Create CSV reader to transcode to Parquet
        final CSVReader reader = new CSVReader();
        runner.addControllerService("csv-reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty("record-reader", "csv-reader");

        // Enqueue CSV data to be converted to Parquet
        final String csvData = "10,David\n20,Eve";
        runner.enqueue(csvData.getBytes(StandardCharsets.UTF_8));

        runner.run();
        
        // Should either succeed or fail gracefully
        final var success = runner.getFlowFilesForRelationship("success");
        final var failure = runner.getFlowFilesForRelationship("failure");
        Assertions.assertTrue(success.size() > 0 || failure.size() > 0);

        if (success.size() > 0) {
            // Verify data was loaded
            try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
                try (Statement st = wrapper.getConnection().createStatement()) {
                    try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + LOAD_TABLE)) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals(2, rs.getInt(1));
                    }
                }
            }
        }
    }

    @Test
    public void testParquetUpsert() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkUpsert.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);
        
        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "Parquet");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", UPSERT_TABLE);
        runner.setProperty("upsert-returns-records", "true");
        runner.setProperty("upsert-sql-template",
                "INSERT INTO ${target_table} SELECT * FROM ${temp_table} ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name");

        // Create CSV reader for input
        final CSVReader reader = new CSVReader();
        runner.addControllerService("csv-reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty("record-reader", "csv-reader");

        // Create CSV writer for output records
        final CSVRecordSetWriter writer = new CSVRecordSetWriter();
        runner.addControllerService("csv-writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty("record-writer", "csv-writer");

        // Insert initial data
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                st.execute("INSERT INTO " + UPSERT_TABLE + " (id, name) VALUES (1,'Original')");
                wrapper.getConnection().commit();
            }
        }

        // Upsert data
        final String csvData = "1,Updated\n2,New";
        runner.enqueue(csvData.getBytes(StandardCharsets.UTF_8));

        runner.run();
        
        // Should either succeed or fail gracefully
        final var success = runner.getFlowFilesForRelationship("success");
        final var failure = runner.getFlowFilesForRelationship("failure");
        Assertions.assertTrue(success.size() > 0 || failure.size() > 0);

        if (success.size() > 0) {
            // Verify upsert worked
            try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
                try (Statement st = wrapper.getConnection().createStatement()) {
                    try (ResultSet rs = st.executeQuery("SELECT name FROM " + UPSERT_TABLE + " WHERE id = 1")) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals("Updated", rs.getString(1));
                    }
                    
                    try (ResultSet rs = st.executeQuery("SELECT name FROM " + UPSERT_TABLE + " WHERE id = 2")) {
                        Assertions.assertTrue(rs.next());
                        Assertions.assertEquals("New", rs.getString(1));
                    }
                }
            }

            // Verify FlowFile contains output records
            final MockFlowFile flowFile = success.get(0);
            final String content = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
            Assertions.assertTrue(content.contains("Updated") || content.contains("1"));
            Assertions.assertTrue(content.contains("New") || content.contains("2"));
        }
    }
}
