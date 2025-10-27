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

import java.nio.charset.StandardCharsets;
import java.sql.Statement;

import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.postgresql.service.util.CredentialManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Consolidated error handling tests for all PostgreSQL processors. Tests various error scenarios and validation that processors handle them
 * correctly.
 */
public class ErrorProcessorsIT {

    private static final CredentialManager.PostgreSQLCredentials CREDENTIALS = CredentialManager.getPostgreSQLCredentials();
    private static final String SCHEMA = "public"; // PostgreSQL default schema
    private static final String LOAD_ERROR_TABLE = SCHEMA + ".nifi_test_load_err";
    private static final String UPSERT_ERROR_TABLE = SCHEMA + ".nifi_test_upsert_err";

    private PostgreSQLConnectionProviderService connectionService;

    private PostgreSQLConnectionProviderService createConnectionProviderService(TestRunner runner,
            CredentialManager.PostgreSQLCredentials credentials) throws Exception {

        runner.setValidateExpressionUsage(false);
        final org.apache.nifi.postgresql.service.PostgreSQLConnectionPool connectionProviderService = new org.apache.nifi.postgresql.service.PostgreSQLConnectionPool();

        runner.addControllerService("postgresqlConnectionProviderService", connectionProviderService);

        runner.setProperty(connectionProviderService, org.apache.nifi.postgresql.service.util.ConnectionPoolSettings.CONNECTION_URL_FORMAT,
                org.apache.nifi.postgresql.service.util.ConnectionUrlFormat.FULL_URL);

        runner.setProperty(connectionProviderService, org.apache.nifi.postgresql.service.util.ConnectionPoolSettings.POSTGRESQL_URL,
                credentials.getJdbcUrl());

        runner.setProperty(connectionProviderService, org.apache.nifi.postgresql.service.util.ConnectionPoolSettings.POSTGRESQL_USER,
                credentials.getUserName());

        runner.setProperty(connectionProviderService, org.apache.nifi.postgresql.service.util.ConnectionPoolSettings.POSTGRESQL_PASSWORD,
                credentials.getPassword());

        runner.setProperty(connectionProviderService, org.apache.nifi.processors.postgresql.util.ConnectionSettings.SSL, "true");

        runner.enableControllerService(connectionProviderService);
        return connectionProviderService;
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Create connection service
        TestRunner tempRunner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        connectionService = createConnectionProviderService(tempRunner, CREDENTIALS);

        // Create schema and tables for error tests
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);

                // Create tables with specific schemas for error testing
                st.execute("CREATE TABLE " + LOAD_ERROR_TABLE + " (id INT PRIMARY KEY, name TEXT)");
                st.execute("CREATE TABLE " + UPSERT_ERROR_TABLE + " (id INT PRIMARY KEY, name TEXT)");

                wrapper.getConnection().commit();
            }
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connectionService != null) {
            try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
                try (Statement st = wrapper.getConnection().createStatement()) {
                    st.execute("DROP TABLE IF EXISTS " + LOAD_ERROR_TABLE);
                    st.execute("DROP TABLE IF EXISTS " + UPSERT_ERROR_TABLE);
                    wrapper.getConnection().commit();
                }
            }
        }
    }

    @Test
    public void testBulkLoadInvalidColumnCount() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "CSV");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", LOAD_ERROR_TABLE);

        // Create CSV reader for parsing input
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.enableControllerService(csvReader);
        runner.setProperty("record-reader", "csv-reader");

        // Enqueue CSV with wrong number of columns (table expects 2, providing 3)
        final String invalidCsvData = "1,Alice,Extra\n2,Bob,ExtraColumn";
        runner.enqueue(invalidCsvData.getBytes(StandardCharsets.UTF_8));

        runner.run();

        // Should route to failure due to column count mismatch
        runner.assertAllFlowFilesTransferred("failure", 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("failure").get(0);
        Assertions.assertNotNull(flowFile.getAttribute("pg.error"));
        Assertions.assertTrue(flowFile.getAttribute("pg.error").contains("column") || flowFile.getAttribute("pg.error").contains("field"));
    }

    @Test
    public void testBulkLoadInvalidDataType() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "CSV");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", LOAD_ERROR_TABLE);

        // Create CSV reader for parsing input
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.enableControllerService(csvReader);
        runner.setProperty("record-reader", "csv-reader");

        // Enqueue CSV with invalid data type (string for integer column)
        final String invalidCsvData = "NotAnInteger,Alice";
        runner.enqueue(invalidCsvData.getBytes(StandardCharsets.UTF_8));

        runner.run();

        // Should route to failure due to data type mismatch
        runner.assertAllFlowFilesTransferred("failure", 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("failure").get(0);
        Assertions.assertNotNull(flowFile.getAttribute("pg.error"));
    }

    @Test
    public void testBulkUpsertDuplicateKey() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkUpsert.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "CSV");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", UPSERT_ERROR_TABLE);
        runner.setProperty("upsert-returns-records", "false");

        // Create CSV reader for parsing input
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.enableControllerService(csvReader);
        runner.setProperty("record-reader", "csv-reader");
        // Note: SQL is now auto-generated. This test now verifies duplicate key handling with ON CONFLICT

        // Insert initial data that will conflict
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                st.execute("INSERT INTO " + UPSERT_ERROR_TABLE + " (id, name) VALUES (1, 'Existing')");
                wrapper.getConnection().commit();
            }
        }

        // Try to insert duplicate key
        final String csvData = "1,Duplicate";
        runner.enqueue(csvData.getBytes(StandardCharsets.UTF_8));

        runner.run();

        // Note: With auto-generated SQL using ON CONFLICT, duplicates are now handled gracefully
        // This should succeed, not fail. Update test to verify success instead.
        runner.assertAllFlowFilesTransferred("success", 1);
    }

    @Test
    public void testBulkUpsertInvalidSQL() throws Exception {
        // NOTE: This test previously tested invalid SQL templates, but upsert now auto-generates SQL.
        // Modified to test upsert to a non-existent table (which will cause SQL error).
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkUpsert.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "CSV");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", "public.non_existent_table_xyz");
        runner.setProperty("upsert-returns-records", "false");

        // Create CSV reader for parsing input
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.enableControllerService(csvReader);
        runner.setProperty("record-reader", "csv-reader");

        final String csvData = "10,Test";
        runner.enqueue(csvData.getBytes(StandardCharsets.UTF_8));

        runner.run();

        // Should route to failure due to table not existing
        runner.assertAllFlowFilesTransferred("failure", 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("failure").get(0);
        Assertions.assertNotNull(flowFile.getAttribute("pg.error"));
        // Just verify we got an error - the specific message can vary
        Assertions.assertTrue(flowFile.getAttribute("pg.error").length() > 0,
                "Should have error message. Got: " + flowFile.getAttribute("pg.error"));
    }
}
