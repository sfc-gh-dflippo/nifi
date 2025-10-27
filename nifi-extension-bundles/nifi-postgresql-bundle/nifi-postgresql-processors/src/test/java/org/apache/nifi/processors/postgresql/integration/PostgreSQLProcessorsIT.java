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
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.postgresql.service.util.CredentialManager;
import org.apache.nifi.processors.postgresql.integration.sql.PostgreSQLUpsertTemplates;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Consolidated integration tests for all PostgreSQL processors. Tests basic functionality, custom queries, and state management.
 *
 * Uses CredentialManager for secure credential resolution: - PostgreSQL: ~/.pg_service.conf [default] service or environment variables
 */
public class PostgreSQLProcessorsIT {

    private static final CredentialManager.PostgreSQLCredentials CREDENTIALS = CredentialManager.getPostgreSQLCredentials();
    private static final String SCHEMA = "public"; // PostgreSQL default schema
    private static final String EXPORT_TABLE = SCHEMA + ".nifi_test_export";
    private static final String LOAD_TABLE = SCHEMA + ".nifi_test_load";
    private static final String UPSERT_TABLE = SCHEMA + ".nifi_test_upsert";
    private static final String COMPLEX_UPSERT_TABLE = SCHEMA + ".nifi_test_complex_upsert";

    private PostgreSQLConnectionProviderService connectionService;

    @BeforeEach
    public void setUp() throws Exception {
        // Create connection service using CredentialManager
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

                // Create complex upsert table with ETL schema
                st.execute("CREATE TABLE " + COMPLEX_UPSERT_TABLE + " (" + "_etl_run_id_ BIGINT, " + "_schema_class_ VARCHAR(255), "
                        + "_context_id_ BIGINT, " + "fulltablename VARCHAR(255), " + "operation_type VARCHAR(50), " + "name VARCHAR(255), "
                        + "primary_key VARCHAR(255), " + "_is_deleted_ BOOLEAN, " + "committedtime TIMESTAMP, " + "extractedtime TIMESTAMP, "
                        + "sortorder BIGINT, " + "loaded_seq BIGINT, " + "src JSONB, " + "_etl_modified_ TIMESTAMP, "
                        + "_source_extracted_ TIMESTAMP, " + "PRIMARY KEY (_context_id_, primary_key)" + ")");

                // Insert test data for export
                st.execute("INSERT INTO " + EXPORT_TABLE + " (id, name) VALUES (1,'A'), (2,'B'), (3,'C')");

                wrapper.getConnection().commit();
            }
        }
    }

    /**
     * Helper method to create connection provider service from CredentialManager credentials
     */
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

    @AfterEach
    public void tearDown() throws Exception {
        if (connectionService != null) {
            try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
                try (Statement st = wrapper.getConnection().createStatement()) {
                    st.execute("DROP TABLE IF EXISTS " + EXPORT_TABLE);
                    st.execute("DROP TABLE IF EXISTS " + LOAD_TABLE);
                    st.execute("DROP TABLE IF EXISTS " + UPSERT_TABLE);
                    st.execute("DROP TABLE IF EXISTS " + COMPLEX_UPSERT_TABLE);
                    wrapper.getConnection().commit();
                }
            }
        }
    }

    @Test
    public void testBulkExportBasic() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("source-table", EXPORT_TABLE);
        runner.setProperty("data-format", "CSV");

        // Create CSV writer
        final CSVRecordSetWriter writer = new CSVRecordSetWriter();
        runner.addControllerService("csv-writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty("record-writer", "csv-writer");

        runner.run();
        runner.assertAllFlowFilesTransferred("success", 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("success").get(0);
        final String content = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);

        // Verify content contains the exported data
        Assertions.assertTrue(content.contains("A"));
        Assertions.assertTrue(content.contains("B"));
        Assertions.assertTrue(content.contains("C"));

        // Verify attributes
        Assertions.assertNotNull(flowFile.getAttribute("record.count"));
    }

    @Test
    public void testBulkExportCustomQuery() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("custom-query", "SELECT * FROM " + EXPORT_TABLE + " WHERE id > 1");
        runner.setProperty("data-format", "CSV");

        // Create CSV writer
        final CSVRecordSetWriter writer = new CSVRecordSetWriter();
        runner.addControllerService("csv-writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty("record-writer", "csv-writer");

        runner.run();
        runner.assertAllFlowFilesTransferred("success", 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("success").get(0);
        final String content = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);

        // Should only contain records where id > 1
        Assertions.assertTrue(content.contains("B"));
        Assertions.assertTrue(content.contains("C"));
    }

    @Test
    public void testBulkExportWithState() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkExport.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("source-table", EXPORT_TABLE);
        runner.setProperty("data-format", "CSV");
        runner.setProperty("maximum-value-columns", "id");

        // Create CSV writer
        final CSVRecordSetWriter writer = new CSVRecordSetWriter();
        runner.addControllerService("csv-writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty("record-writer", "csv-writer");

        runner.run();
        runner.assertAllFlowFilesTransferred("success", 1);

        // Verify state management works
        final StateMap state = runner.getStateManager().getState(Scope.CLUSTER);
        Assertions.assertNotNull(state);
    }

    @Test
    public void testBulkLoadBasic() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkLoad.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "CSV");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", LOAD_TABLE);
        runner.setProperty("csv-header", "true"); // CSV data includes header row

        // Create CSV reader for parsing input
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.enableControllerService(csvReader);
        runner.setProperty("record-reader", "csv-reader");

        final String csvData = "id,name\n1,Alice\n2,Bob\n3,Charlie";
        runner.enqueue(csvData.getBytes(StandardCharsets.UTF_8));

        runner.run();

        // Debug logging for troubleshooting
        System.err.println("Success FlowFiles: " + runner.getFlowFilesForRelationship("success").size());
        System.err.println("Failure FlowFiles: " + runner.getFlowFilesForRelationship("failure").size());

        if (!runner.getFlowFilesForRelationship("failure").isEmpty()) {
            final MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship("failure").get(0);
            final String errorMsg = failedFlowFile.getAttribute("pg.error");
            System.err.println("testBulkLoadBasic failed with error: " + errorMsg);
            // Print all attributes for debugging
            System.err.println("All attributes: " + failedFlowFile.getAttributes());
        }

        runner.assertAllFlowFilesTransferred("success", 1);

        // Verify data was loaded into the database
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + LOAD_TABLE)) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(3, rs.getInt(1));
                }

                try (ResultSet rs = st.executeQuery("SELECT name FROM " + LOAD_TABLE + " WHERE id = 1")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals("Alice", rs.getString(1));
                }
            }
        }

        // Verify FlowFile attributes
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("success").get(0);
        Assertions.assertNotNull(flowFile.getAttribute("record.count"));
    }

    @Test
    public void testBulkUpsertBasic() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkUpsert.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "CSV");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", UPSERT_TABLE);
        runner.setProperty("csv-header", "true"); // CSV data includes header row
        runner.setProperty("upsert-returns-records", "false");
        // Note: upsert-sql-template removed - SQL is now auto-generated based on table metadata

        // Create CSV reader for parsing input
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.enableControllerService(csvReader);
        runner.setProperty("record-reader", "csv-reader");

        // Insert initial data
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                st.execute("INSERT INTO " + UPSERT_TABLE + " (id, name) VALUES (1,'Original')");
                wrapper.getConnection().commit();
            }
        }

        // Upsert data: update existing record and insert new one (include header)
        final String csvData = "id,name\n1,Updated\n2,New";
        runner.enqueue(csvData.getBytes(StandardCharsets.UTF_8));

        runner.run();

        // Debug logging for troubleshooting
        if (!runner.getFlowFilesForRelationship("failure").isEmpty()) {
            final MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship("failure").get(0);
            final String errorMsg = failedFlowFile.getAttribute("pg.error");
            System.err.println("testBulkUpsertBasic failed with error: " + errorMsg);
        }

        runner.assertAllFlowFilesTransferred("success", 1);

        // Verify the upsert worked
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                // Check updated record
                try (ResultSet rs = st.executeQuery("SELECT name FROM " + UPSERT_TABLE + " WHERE id = 1")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals("Updated", rs.getString(1));
                }

                // Check new record
                try (ResultSet rs = st.executeQuery("SELECT name FROM " + UPSERT_TABLE + " WHERE id = 2")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals("New", rs.getString(1));
                }
            }
        }

        // Verify FlowFile attributes
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("success").get(0);
        Assertions.assertNotNull(flowFile.getAttribute("record.count"));
    }

    @Test
    public void testBulkUpsertComplexETLDeduplication() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(PostgreSQLBulkUpsert.class);
        connectionService = createConnectionProviderService(runner, CREDENTIALS);

        runner.setProperty("postgresql-connection-provider", "postgresqlConnectionProviderService");
        runner.setProperty("data-format", "CSV");
        runner.setProperty("stream-incoming-file", "false");
        runner.setProperty("target-table", COMPLEX_UPSERT_TABLE);
        runner.setProperty("csv-header", "true"); // CSV data includes header row
        runner.setProperty("upsert-returns-records", "true");

        // Note: upsert-sql-template removed - SQL is now auto-generated based on table metadata
        // This includes JSONB concatenation for merge operations automatically

        // Create CSV reader and writer for parsing input and output
        final CSVReader csvReader = new CSVReader();
        runner.addControllerService("csv-reader", csvReader);
        runner.enableControllerService(csvReader);
        runner.setProperty("record-reader", "csv-reader");

        final CSVRecordSetWriter csvWriter = new CSVRecordSetWriter();
        runner.addControllerService("csv-writer", csvWriter);
        runner.enableControllerService(csvWriter);
        runner.setProperty("record-writer", "csv-writer");

        // Insert initial data to test conflict resolution and JSONB merge
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                st.execute("INSERT INTO " + COMPLEX_UPSERT_TABLE + " "
                        + "(_etl_run_id_, _schema_class_, _context_id_, fulltablename, operation_type, name, primary_key, "
                        + "_is_deleted_, committedtime, extractedtime, sortorder, loaded_seq, src, _etl_modified_, _source_extracted_) " + "VALUES "
                        + "(100, 'TestSchema', 1, 'test_table', 'INSERT', 'Initial Record', 'key1', false, "
                        + "'2024-01-01 10:00:00', '2024-01-01 09:00:00', 1, 1, "
                        + "'{\"field1\": \"initial_value\", \"field2\": 100}', '2024-01-01 10:00:00', '2024-01-01 09:00:00')");
                wrapper.getConnection().commit();
            }
        }

        // Create test data for complex upsert testing
        // This tests upsert logic with JSONB merge functionality
        // Note: Each record must have unique (context_id, primary_key) for temp table
        final String csvData = "_etl_run_id_,_schema_class_,_context_id_,fulltablename,operation_type,name,primary_key,"
                + "_is_deleted_,committedtime,extractedtime,sortorder,loaded_seq,src,_etl_modified_,_source_extracted_\n" +

                // Update existing entity (context_id=1, primary_key=key1) - will merge with
                // initial record
                "102,TestSchema,1,test_table,UPDATE,Latest Record,key1,false," + "2024-01-01 12:00:00,2024-01-01 11:30:00,3,3,"
                + "\"{\"\"field1\"\": \"\"updated_value\"\", \"\"field2\"\": 200, \"\"field4\"\": true}\",2024-01-01 12:00:00,2024-01-01 11:30:00\n" +

                // New entity (context_id=2, primary_key=key2)
                "103,TestSchema,2,test_table,INSERT,New Entity,key2,false," + "2024-01-01 13:00:00,2024-01-01 12:30:00,1,1,"
                + "\"{\"\"entity_type\"\": \"\"new\"\", \"\"status\"\": \"\"active\"\"}\",2024-01-01 13:00:00,2024-01-01 12:30:00\n";

        runner.enqueue(csvData.getBytes(StandardCharsets.UTF_8));
        runner.run();

        // Debug logging for troubleshooting
        if (!runner.getFlowFilesForRelationship("failure").isEmpty()) {
            final MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship("failure").get(0);
            final String errorMsg = failedFlowFile.getAttribute("pg.error");
            System.err.println("testBulkUpsertComplexETLDeduplication failed with error: " + errorMsg);
            System.err.println("All attributes: " + failedFlowFile.getAttributes());
        }

        runner.assertAllFlowFilesTransferred("success", 1);

        // Verify the complex upsert worked correctly
        try (PostgreSQLConnectionWrapper wrapper = connectionService.getPostgreSQLConnection()) {
            try (Statement st = wrapper.getConnection().createStatement()) {
                // Check that we have exactly 2 records (one updated, one new)
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + COMPLEX_UPSERT_TABLE)) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(2, rs.getInt(1), "Should have exactly 2 records after upsert");
                }

                // Verify the updated record has the latest values and merged JSONB
                try (ResultSet rs = st.executeQuery("SELECT _etl_run_id_, name, operation_type, src FROM " + COMPLEX_UPSERT_TABLE
                        + " WHERE _context_id_ = 1 AND primary_key = 'key1'")) {
                    Assertions.assertTrue(rs.next());

                    // Should have latest _etl_run_id_ (102) and name from most recent record
                    Assertions.assertEquals(102, rs.getLong("_etl_run_id_"));
                    Assertions.assertEquals("Latest Record", rs.getString("name"));
                    Assertions.assertEquals("UPDATE", rs.getString("operation_type"));

                    // Verify JSONB merge: should contain merged fields from initial and update
                    // records
                    // Note: PostgreSQL JSONB format includes spaces after colons
                    String srcJson = rs.getString("src");
                    Assertions.assertTrue(srcJson.contains("\"field1\": \"updated_value\"") || srcJson.contains("\"field1\":\"updated_value\""),
                            "Should contain updated field1 from update record. Got: " + srcJson);
                    Assertions.assertTrue(srcJson.contains("\"field2\": 200") || srcJson.contains("\"field2\":200"),
                            "Should contain updated field2 from update record. Got: " + srcJson);
                    Assertions.assertTrue(srcJson.contains("\"field4\": true") || srcJson.contains("\"field4\":true"),
                            "Should contain new field4 from update record. Got: " + srcJson);
                }

                // Verify the new record
                try (ResultSet rs = st.executeQuery(
                        "SELECT _etl_run_id_, name, src FROM " + COMPLEX_UPSERT_TABLE + " WHERE _context_id_ = 2 AND primary_key = 'key2'")) {
                    Assertions.assertTrue(rs.next());
                    Assertions.assertEquals(103, rs.getLong("_etl_run_id_"));
                    Assertions.assertEquals("New Entity", rs.getString("name"));

                    String srcJson = rs.getString("src");
                    Assertions.assertTrue(srcJson.contains("\"entity_type\": \"new\"") || srcJson.contains("\"entity_type\":\"new\""));
                    Assertions.assertTrue(srcJson.contains("\"status\": \"active\"") || srcJson.contains("\"status\":\"active\""));
                }
            }
        }

        // Verify FlowFile attributes and returned records
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship("success").get(0);
        Assertions.assertNotNull(flowFile.getAttribute("record.count"));

        // Verify the output contains the upserted records
        final String outputContent = new String(flowFile.toByteArray(), StandardCharsets.UTF_8);
        Assertions.assertTrue(outputContent.contains("Latest Record"), "Output should contain the updated record");
        Assertions.assertTrue(outputContent.contains("New Entity"), "Output should contain the new record");
    }

}
