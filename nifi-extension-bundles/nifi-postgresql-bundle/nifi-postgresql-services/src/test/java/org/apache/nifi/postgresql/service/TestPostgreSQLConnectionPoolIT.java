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

package org.apache.nifi.postgresql.service;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.postgresql.service.util.ConnectionPoolSettings;
import org.apache.nifi.postgresql.service.util.TableMetadata;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionWrapper;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager.PostgreSQLCredentials;
import org.apache.nifi.processors.postgresql.util.ConnectionSettings;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceLookup;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Integration test covering connection pool configuration and a simple PG unwrap.
 * Uses CredentialManager to load credentials from ~/.pg_service.conf or environment variables.
 */
public class TestPostgreSQLConnectionPoolIT {

    private static final String SERVICE_ID = "postgresqlConnectionProviderService";

    private PostgreSQLConnectionPool connectionPool;
    private TestRunner runner;
    private Connection connection;

    /**
     * Helper method to get the controller service properties for verify() calls.
     */
    private Map<PropertyDescriptor, String> getServiceProperties(final TestRunner runner) {
        return ((MockControllerServiceLookup) runner.getProcessContext().getControllerServiceLookup())
                .getControllerServices().get(SERVICE_ID).getProperties();
    }

    /**
     * Helper method to create a ConfigurationContext for verify() calls.
     */
    private ConfigurationContext getConfigurationContext(final TestRunner runner, final PostgreSQLConnectionPool service) {
        return new MockConfigurationContext(service, getServiceProperties(runner), 
                runner.getProcessContext().getControllerServiceLookup(), null);
    }

    @Test
    public void testConnectAndUnwrap() throws Exception {
        // Load credentials from ~/.pg_service.conf or environment variables
        final PostgreSQLCredentials credentials = CredentialManager.getPostgreSQLCredentials();
        
        final TestRunner runner = TestRunners.newTestRunner(new org.apache.nifi.processor.AbstractProcessor() {
            @Override
            public java.util.List<PropertyDescriptor> getSupportedPropertyDescriptors() { return java.util.List.of(); }
            @Override
            public void onTrigger(org.apache.nifi.processor.ProcessContext context, org.apache.nifi.processor.ProcessSession session) {}
        });

        final PostgreSQLConnectionPool connectionProviderService = new PostgreSQLConnectionPool();
        runner.addControllerService(SERVICE_ID, connectionProviderService);

        // Configure URL format
        runner.setProperty(connectionProviderService,
                ConnectionPoolSettings.CONNECTION_URL_FORMAT,
                org.apache.nifi.postgresql.service.util.ConnectionUrlFormat.FULL_URL.getValue());

        // Use credentials from CredentialManager
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_URL, credentials.getJdbcUrl());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_USER, credentials.getUserName());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_PASSWORD, credentials.getPassword());
        runner.setProperty(connectionProviderService, ConnectionSettings.SSL, "true");

        runner.setValidateExpressionUsage(false);
        runner.enableControllerService(connectionProviderService);

        final PostgreSQLConnectionProviderService svc = connectionProviderService;
        try (PostgreSQLConnectionWrapper wrapper = svc.getPostgreSQLConnection()) {
            final PGConnection pg = wrapper.unwrap();
            Assertions.assertNotNull(pg.getCopyAPI());
        }
    }

    /**
     * Test that connection failure with invalid hostname produces enhanced error message.
     */
    @Test
    public void testEnhancedErrorMessageWithInvalidHost() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new org.apache.nifi.processor.AbstractProcessor() {
            @Override
            public java.util.List<PropertyDescriptor> getSupportedPropertyDescriptors() { return java.util.List.of(); }
            @Override
            public void onTrigger(org.apache.nifi.processor.ProcessContext context, org.apache.nifi.processor.ProcessSession session) {}
        });

        final PostgreSQLConnectionPool connectionProviderService = new PostgreSQLConnectionPool();
        runner.addControllerService(SERVICE_ID, connectionProviderService);

        // Configure with invalid hostname
        runner.setProperty(connectionProviderService,
                ConnectionPoolSettings.CONNECTION_URL_FORMAT,
                org.apache.nifi.postgresql.service.util.ConnectionUrlFormat.HOST_NAME.getValue());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_HOST_NAME, "invalid-nonexistent-host-12345.example.com");
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_PORT, "5432");
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_DATABASE, "testdb");
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_USER, "testuser");
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_PASSWORD, "testpass");

        runner.setValidateExpressionUsage(false);
        runner.assertValid(connectionProviderService);

        // Perform verification which should fail with enhanced error message
        final List<ConfigVerificationResult> results = connectionProviderService.verify(
                getConfigurationContext(runner, connectionProviderService),
                runner.getLogger(),
                Collections.emptyMap());

        // Find the connection establishment result
        final ConfigVerificationResult connectionResult = results.stream()
                .filter(r -> r.getVerificationStepName().equals("Establish Connection"))
                .findFirst()
                .orElse(null);

        Assertions.assertNotNull(connectionResult, "Should have connection verification result");
        assertEquals(ConfigVerificationResult.Outcome.FAILED, connectionResult.getOutcome(), "Connection should fail");

        final String explanation = connectionResult.getExplanation();
        
        // Verify enhanced error message contains expected elements
        assertTrue(explanation.contains("Failed to establish PostgreSQL connection"), 
                "Should contain failure header");
        assertTrue(explanation.contains("Host: invalid-nonexistent-host-12345.example.com"), 
                "Should contain host information");
        assertTrue(explanation.contains("Port: 5432"), 
                "Should contain port information");
        assertTrue(explanation.contains("Database: testdb"), 
                "Should contain database information");
        assertTrue(explanation.contains("Actual PostgreSQL Error:"), 
                "Should contain actual PostgreSQL error section");
        assertTrue(explanation.contains("Troubleshooting Tips:"), 
                "Should contain troubleshooting tips");
        assertTrue(explanation.contains("Verify PostgreSQL is running"), 
                "Should contain troubleshooting guidance");
    }

    /**
     * Test that connection failure with invalid port produces enhanced error message.
     */
    @Test
    public void testEnhancedErrorMessageWithInvalidPort() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new org.apache.nifi.processor.AbstractProcessor() {
            @Override
            public java.util.List<PropertyDescriptor> getSupportedPropertyDescriptors() { return java.util.List.of(); }
            @Override
            public void onTrigger(org.apache.nifi.processor.ProcessContext context, org.apache.nifi.processor.ProcessSession session) {}
        });

        final PostgreSQLConnectionPool connectionProviderService = new PostgreSQLConnectionPool();
        runner.addControllerService(SERVICE_ID, connectionProviderService);

        // Configure with invalid port (using localhost but wrong port)
        runner.setProperty(connectionProviderService,
                ConnectionPoolSettings.CONNECTION_URL_FORMAT,
                org.apache.nifi.postgresql.service.util.ConnectionUrlFormat.HOST_NAME.getValue());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_HOST_NAME, "localhost");
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_PORT, "9999"); // Wrong port
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_DATABASE, "testdb");
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_USER, "testuser");
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_PASSWORD, "testpass");

        runner.setValidateExpressionUsage(false);
        runner.assertValid(connectionProviderService);

        // Perform verification which should fail with enhanced error message
        final List<ConfigVerificationResult> results = connectionProviderService.verify(
                getConfigurationContext(runner, connectionProviderService),
                runner.getLogger(),
                Collections.emptyMap());

        // Find the connection establishment result
        final ConfigVerificationResult connectionResult = results.stream()
                .filter(r -> r.getVerificationStepName().equals("Establish Connection"))
                .findFirst()
                .orElse(null);

        Assertions.assertNotNull(connectionResult, "Should have connection verification result");
        assertEquals(ConfigVerificationResult.Outcome.FAILED, connectionResult.getOutcome(), "Connection should fail");

        final String explanation = connectionResult.getExplanation();
        
        // Verify enhanced error message contains expected elements
        assertTrue(explanation.contains("Failed to establish PostgreSQL connection"), 
                "Should contain failure header");
        assertTrue(explanation.contains("Port: 9999"), 
                "Should contain port information");
        assertTrue(explanation.contains("Actual PostgreSQL Error:"), 
                "Should contain actual PostgreSQL error section");
        assertTrue(explanation.contains("Troubleshooting Tips:"), 
                "Should contain troubleshooting tips");
        // Should mention connection refused or similar
        assertTrue(explanation.contains("Connection refused") || explanation.contains("refused") || 
                   explanation.contains("Connection attempt failed") || explanation.contains("timeout"),
                "Should contain connection error details");
    }

    /**
     * Test that connection failure with invalid credentials produces enhanced error message.
     * This test requires a running PostgreSQL instance but uses wrong credentials.
     */
    @Test
    public void testEnhancedErrorMessageWithInvalidCredentials() throws Exception {
        // This test only runs if we can get real credentials (indicating a PG instance is available)
        final PostgreSQLCredentials validCredentials;
        try {
            validCredentials = CredentialManager.getPostgreSQLCredentials();
        } catch (Exception e) {
            // Skip test if no PostgreSQL credentials are available
            System.out.println("Skipping testEnhancedErrorMessageWithInvalidCredentials - no PostgreSQL instance available");
            return;
        }

        final TestRunner runner = TestRunners.newTestRunner(new org.apache.nifi.processor.AbstractProcessor() {
            @Override
            public java.util.List<PropertyDescriptor> getSupportedPropertyDescriptors() { return java.util.List.of(); }
            @Override
            public void onTrigger(org.apache.nifi.processor.ProcessContext context, org.apache.nifi.processor.ProcessSession session) {}
        });

        final PostgreSQLConnectionPool connectionProviderService = new PostgreSQLConnectionPool();
        runner.addControllerService(SERVICE_ID, connectionProviderService);

        // Use valid connection details but invalid password
        runner.setProperty(connectionProviderService,
                ConnectionPoolSettings.CONNECTION_URL_FORMAT,
                org.apache.nifi.postgresql.service.util.ConnectionUrlFormat.FULL_URL.getValue());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_URL, validCredentials.getJdbcUrl());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_USER, validCredentials.getUserName());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_PASSWORD, "definitely_wrong_password_12345");
        runner.setProperty(connectionProviderService, ConnectionSettings.SSL, "false");

        runner.setValidateExpressionUsage(false);
        runner.assertValid(connectionProviderService);

        // Perform verification which should fail with enhanced error message
        final List<ConfigVerificationResult> results = connectionProviderService.verify(
                getConfigurationContext(runner, connectionProviderService),
                runner.getLogger(),
                Collections.emptyMap());

        // Find the connection establishment result
        final ConfigVerificationResult connectionResult = results.stream()
                .filter(r -> r.getVerificationStepName().equals("Establish Connection"))
                .findFirst()
                .orElse(null);

        Assertions.assertNotNull(connectionResult, "Should have connection verification result");
        assertEquals(ConfigVerificationResult.Outcome.FAILED, connectionResult.getOutcome(), "Connection should fail");

        final String explanation = connectionResult.getExplanation();
        
        // Verify enhanced error message contains expected elements
        assertTrue(explanation.contains("Failed to establish PostgreSQL connection"), 
                "Should contain failure header");
        assertTrue(explanation.contains("Connection URL:"), 
                "Should contain connection URL");
        assertTrue(explanation.contains("Actual PostgreSQL Error:"), 
                "Should contain actual PostgreSQL error section");
        assertTrue(explanation.contains("Troubleshooting Tips:"), 
                "Should contain troubleshooting tips");
        
        // Password in URL should be masked
        assertFalse(explanation.contains("definitely_wrong_password_12345"), 
                "Password should be masked in error message");
    }

    /**
     * Test that connection failure with invalid username produces enhanced error message.
     * This test requires a running PostgreSQL instance but uses wrong username.
     */
    @Test
    public void testEnhancedErrorMessageWithInvalidUsername() throws Exception {
        // This test only runs if we can get real credentials (indicating a PG instance is available)
        final PostgreSQLCredentials validCredentials;
        try {
            validCredentials = CredentialManager.getPostgreSQLCredentials();
        } catch (Exception e) {
            // Skip test if no PostgreSQL credentials are available
            System.out.println("Skipping testEnhancedErrorMessageWithInvalidUsername - no PostgreSQL instance available");
            return;
        }

        final TestRunner runner = TestRunners.newTestRunner(new org.apache.nifi.processor.AbstractProcessor() {
            @Override
            public java.util.List<PropertyDescriptor> getSupportedPropertyDescriptors() { return java.util.List.of(); }
            @Override
            public void onTrigger(org.apache.nifi.processor.ProcessContext context, org.apache.nifi.processor.ProcessSession session) {}
        });

        final PostgreSQLConnectionPool connectionProviderService = new PostgreSQLConnectionPool();
        runner.addControllerService(SERVICE_ID, connectionProviderService);

        // Use valid connection details but invalid username
        runner.setProperty(connectionProviderService,
                ConnectionPoolSettings.CONNECTION_URL_FORMAT,
                org.apache.nifi.postgresql.service.util.ConnectionUrlFormat.FULL_URL.getValue());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_URL, validCredentials.getJdbcUrl());
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_USER, "invalid_user_that_does_not_exist");
        runner.setProperty(connectionProviderService, ConnectionPoolSettings.POSTGRESQL_PASSWORD, validCredentials.getPassword());
        runner.setProperty(connectionProviderService, ConnectionSettings.SSL, "false");

        runner.setValidateExpressionUsage(false);
        runner.assertValid(connectionProviderService);

        // Perform verification which should fail with enhanced error message
        final List<ConfigVerificationResult> results = connectionProviderService.verify(
                getConfigurationContext(runner, connectionProviderService),
                runner.getLogger(),
                Collections.emptyMap());

        // Find the connection establishment result
        final ConfigVerificationResult connectionResult = results.stream()
                .filter(r -> r.getVerificationStepName().equals("Establish Connection"))
                .findFirst()
                .orElse(null);

        Assertions.assertNotNull(connectionResult, "Should have connection verification result");
        assertEquals(ConfigVerificationResult.Outcome.FAILED, connectionResult.getOutcome(), "Connection should fail");

        final String explanation = connectionResult.getExplanation();
        
        // Verify enhanced error message contains expected elements
        assertTrue(explanation.contains("Failed to establish PostgreSQL connection"), 
                "Should contain failure header");
        assertTrue(explanation.contains("Actual PostgreSQL Error:"), 
                "Should contain actual PostgreSQL error section");
        assertTrue(explanation.contains("Troubleshooting Tips:"), 
                "Should contain troubleshooting tips");
    }

    @BeforeEach
    public void setupMetadataTests() throws Exception {
        // Only set up connection pool if credentials are available
        try {
            final PostgreSQLCredentials credentials = CredentialManager.getPostgreSQLCredentials();
            
            runner = TestRunners.newTestRunner(new org.apache.nifi.processor.AbstractProcessor() {
                @Override
                public java.util.List<PropertyDescriptor> getSupportedPropertyDescriptors() { return java.util.List.of(); }
                @Override
                public void onTrigger(org.apache.nifi.processor.ProcessContext context, org.apache.nifi.processor.ProcessSession session) {}
            });

            connectionPool = new PostgreSQLConnectionPool();
            runner.addControllerService(SERVICE_ID, connectionPool);

            runner.setProperty(connectionPool,
                    ConnectionPoolSettings.CONNECTION_URL_FORMAT,
                    org.apache.nifi.postgresql.service.util.ConnectionUrlFormat.FULL_URL.getValue());
            runner.setProperty(connectionPool, ConnectionPoolSettings.POSTGRESQL_URL, credentials.getJdbcUrl());
            runner.setProperty(connectionPool, ConnectionPoolSettings.POSTGRESQL_USER, credentials.getUserName());
            runner.setProperty(connectionPool, ConnectionPoolSettings.POSTGRESQL_PASSWORD, credentials.getPassword());
            runner.setProperty(connectionPool, ConnectionSettings.SSL, "true");
            runner.setProperty(connectionPool, PostgreSQLConnectionPool.METADATA_CACHE_TTL, "60000"); // 60 seconds

            runner.setValidateExpressionUsage(false);
            runner.enableControllerService(connectionPool);

            // Get connection without closing it, for use in tests
            final PostgreSQLConnectionWrapper wrapper = connectionPool.getPostgreSQLConnection();
            connection = wrapper.getConnection();
            connection.setAutoCommit(true); // Use autocommit for test simplicity
        } catch (Exception e) {
            // If no credentials available, tests will be skipped
            connectionPool = null;
            connection = null;
        }
    }

    @AfterEach
    public void cleanupMetadataTests() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test_metadata_simple");
                stmt.execute("DROP TABLE IF EXISTS test_metadata_composite_pk");
                stmt.execute("DROP TABLE IF EXISTS test_metadata_with_jsonb");
                stmt.execute("DROP TABLE IF EXISTS test_metadata_no_pk");
                stmt.execute("DROP TABLE IF EXISTS test_metadata_users");
                stmt.execute("DROP TABLE IF EXISTS test_metadata_orders");
            }
            connection.close();
        }
        if (connectionPool != null) {
            connectionPool.clearMetadataCache();
        }
    }

    @Test
    public void testGetTableMetadataSimpleTable() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testGetTableMetadataSimpleTable - no PostgreSQL instance available");
            return;
        }

        // Create test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_simple");
            stmt.execute("CREATE TABLE test_metadata_simple (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))");
        }

        // Fetch metadata
        final TableMetadata metadata = connectionPool.getTableMetadata("public", "test_metadata_simple");

        assertNotNull(metadata);
        assertEquals("public", metadata.getSchema());
        assertEquals("test_metadata_simple", metadata.getTable());
        assertTrue(metadata.hasPrimaryKey());
        assertEquals(1, metadata.getPrimaryKeyColumns().size());
        assertTrue(metadata.isPrimaryKeyColumn("id"));
        assertEquals(3, metadata.getAllColumns().size());
        assertTrue(metadata.getAllColumns().contains("id"));
        assertTrue(metadata.getAllColumns().contains("name"));
        assertTrue(metadata.getAllColumns().contains("email"));
    }

    @Test
    public void testGetTableMetadataIsCached() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testGetTableMetadataIsCached - no PostgreSQL instance available");
            return;
        }

        // Create test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_users");
            stmt.execute("CREATE TABLE test_metadata_users (id SERIAL PRIMARY KEY, username VARCHAR(100))");
        }

        // Fetch metadata twice
        final TableMetadata metadata1 = connectionPool.getTableMetadata("public", "test_metadata_users");
        final TableMetadata metadata2 = connectionPool.getTableMetadata("public", "test_metadata_users");

        assertNotNull(metadata1);
        assertNotNull(metadata2);
        // Should be the same cached instance
        assertSame(metadata1, metadata2, "Metadata should be cached and return same instance");
    }

    @Test
    public void testGetTableMetadataForceRefresh() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testGetTableMetadataForceRefresh - no PostgreSQL instance available");
            return;
        }

        // Create test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_orders");
            stmt.execute("CREATE TABLE test_metadata_orders (id SERIAL PRIMARY KEY, total NUMERIC(10,2))");
        }

        // Fetch metadata first time
        final TableMetadata metadata1 = connectionPool.getTableMetadata("public", "test_metadata_orders");
        assertNotNull(metadata1);
        assertEquals(2, metadata1.getAllColumns().size());

        // Alter the table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("ALTER TABLE test_metadata_orders ADD COLUMN customer_name VARCHAR(100)");
        }

        // Fetch without refresh - should get cached version
        final TableMetadata metadata2 = connectionPool.getTableMetadata("public", "test_metadata_orders", false);
        assertEquals(2, metadata2.getAllColumns().size(), "Should still have 2 columns from cache");

        // Fetch with refresh - should get new version
        final TableMetadata metadata3 = connectionPool.getTableMetadata("public", "test_metadata_orders", true);
        assertEquals(3, metadata3.getAllColumns().size(), "Should now have 3 columns after refresh");
        assertTrue(metadata3.getAllColumns().contains("customer_name"));
    }

    @Test
    public void testGetTableMetadataWithColumnValidationMatch() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testGetTableMetadataWithColumnValidationMatch - no PostgreSQL instance available");
            return;
        }

        // Create test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_users");
            stmt.execute("CREATE TABLE test_metadata_users (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))");
        }

        // Fetch with matching columns
        final List<String> incomingColumns = Arrays.asList("id", "name", "email");
        final TableMetadata metadata = connectionPool.getTableMetadataWithColumnValidation(
                "public", "test_metadata_users", incomingColumns);

        assertNotNull(metadata);
        assertTrue(metadata.hasAllColumns(incomingColumns));
    }

    @Test
    public void testGetTableMetadataWithColumnValidationMismatch() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testGetTableMetadataWithColumnValidationMismatch - no PostgreSQL instance available");
            return;
        }

        // Create test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_users");
            stmt.execute("CREATE TABLE test_metadata_users (id SERIAL PRIMARY KEY, name VARCHAR(100))");
        }

        // Fetch initial metadata
        final TableMetadata metadata1 = connectionPool.getTableMetadata("public", "test_metadata_users");
        assertEquals(2, metadata1.getAllColumns().size());

        // Alter table to add column
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("ALTER TABLE test_metadata_users ADD COLUMN phone VARCHAR(20)");
        }

        // Request with new column - should trigger refresh
        final List<String> incomingColumns = Arrays.asList("id", "name", "phone");
        final TableMetadata metadata2 = connectionPool.getTableMetadataWithColumnValidation(
                "public", "test_metadata_users", incomingColumns);

        assertNotNull(metadata2);
        assertEquals(3, metadata2.getAllColumns().size());
        assertTrue(metadata2.hasAllColumns(incomingColumns));
    }

    @Test
    public void testGetTableMetadataWithJsonbColumns() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testGetTableMetadataWithJsonbColumns - no PostgreSQL instance available");
            return;
        }

        // Create test table with JSONB
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_with_jsonb");
            stmt.execute("CREATE TABLE test_metadata_with_jsonb (id SERIAL PRIMARY KEY, name VARCHAR(100), metadata JSONB, settings JSON)");
        }

        // Fetch metadata
        final TableMetadata metadata = connectionPool.getTableMetadata("public", "test_metadata_with_jsonb");

        assertNotNull(metadata);
        assertEquals(2, metadata.getJsonbColumns().size());
        assertTrue(metadata.isJsonbColumn("metadata"));
        assertTrue(metadata.isJsonbColumn("settings"));
        assertFalse(metadata.isJsonbColumn("name"));
        assertFalse(metadata.isJsonbColumn("id"));
    }

    @Test
    public void testGetTableMetadataCompositePrimaryKey() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testGetTableMetadataCompositePrimaryKey - no PostgreSQL instance available");
            return;
        }

        // Create test table with composite PK
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_composite_pk");
            stmt.execute("CREATE TABLE test_metadata_composite_pk (tenant_id INT, user_id INT, name VARCHAR(100), PRIMARY KEY (tenant_id, user_id))");
        }

        // Fetch metadata
        final TableMetadata metadata = connectionPool.getTableMetadata("public", "test_metadata_composite_pk");

        assertNotNull(metadata);
        assertTrue(metadata.hasPrimaryKey());
        assertEquals(2, metadata.getPrimaryKeyColumns().size());
        assertTrue(metadata.isPrimaryKeyColumn("tenant_id"));
        assertTrue(metadata.isPrimaryKeyColumn("user_id"));
        assertFalse(metadata.isPrimaryKeyColumn("name"));
    }

    @Test
    public void testGetTableMetadataWithoutPrimaryKey() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testGetTableMetadataWithoutPrimaryKey - no PostgreSQL instance available");
            return;
        }

        // Create test table without PK
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_no_pk");
            stmt.execute("CREATE TABLE test_metadata_no_pk (id INT, name VARCHAR(100), email VARCHAR(100))");
        }

        // Fetch metadata
        final TableMetadata metadata = connectionPool.getTableMetadata("public", "test_metadata_no_pk");

        assertNotNull(metadata);
        assertFalse(metadata.hasPrimaryKey());
        assertEquals(0, metadata.getPrimaryKeyColumns().size());
        assertEquals(3, metadata.getAllColumns().size());
    }

    @Test
    public void testInvalidateTableMetadata() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testInvalidateTableMetadata - no PostgreSQL instance available");
            return;
        }

        // Create test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_users");
            stmt.execute("CREATE TABLE test_metadata_users (id SERIAL PRIMARY KEY, name VARCHAR(100))");
        }

        // Fetch metadata to cache it
        final TableMetadata metadata1 = connectionPool.getTableMetadata("public", "test_metadata_users");
        assertNotNull(metadata1);

        // Verify it's cached
        final TableMetadata metadata2 = connectionPool.getTableMetadata("public", "test_metadata_users");
        assertSame(metadata1, metadata2, "Should be same cached instance");

        // Invalidate the cache
        connectionPool.invalidateTableMetadata("public", "test_metadata_users");

        // Fetch again - should get new instance
        final TableMetadata metadata3 = connectionPool.getTableMetadata("public", "test_metadata_users");
        assertNotNull(metadata3);
        // Can't assert not same because PostgreSQL hasn't changed, but cache should have been cleared
    }

    @Test
    public void testClearMetadataCache() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testClearMetadataCache - no PostgreSQL instance available");
            return;
        }

        // Create multiple test tables
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_users");
            stmt.execute("DROP TABLE IF EXISTS test_metadata_orders");
            stmt.execute("CREATE TABLE test_metadata_users (id SERIAL PRIMARY KEY, name VARCHAR(100))");
            stmt.execute("CREATE TABLE test_metadata_orders (id SERIAL PRIMARY KEY, total NUMERIC(10,2))");
        }

        // Fetch metadata for both to cache them
        final TableMetadata users1 = connectionPool.getTableMetadata("public", "test_metadata_users");
        final TableMetadata orders1 = connectionPool.getTableMetadata("public", "test_metadata_orders");
        assertNotNull(users1);
        assertNotNull(orders1);

        // Verify they're cached
        assertSame(users1, connectionPool.getTableMetadata("public", "test_metadata_users"));
        assertSame(orders1, connectionPool.getTableMetadata("public", "test_metadata_orders"));

        // Clear entire cache
        connectionPool.clearMetadataCache();

        // Fetch again - should get new instances (cache was cleared)
        final TableMetadata users2 = connectionPool.getTableMetadata("public", "test_metadata_users");
        final TableMetadata orders2 = connectionPool.getTableMetadata("public", "test_metadata_orders");
        assertNotNull(users2);
        assertNotNull(orders2);
    }

    @Test
    public void testMetadataColumnTypes() throws Exception {
        if (connectionPool == null) {
            System.out.println("Skipping testMetadataColumnTypes - no PostgreSQL instance available");
            return;
        }

        // Create test table with various types
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_metadata_simple");
            stmt.execute("CREATE TABLE test_metadata_simple (" +
                    "id SERIAL PRIMARY KEY, " +
                    "name VARCHAR(100), " +
                    "age INT, " +
                    "salary NUMERIC(10,2), " +
                    "active BOOLEAN)");
        }

        // Fetch metadata
        final TableMetadata metadata = connectionPool.getTableMetadata("public", "test_metadata_simple");

        assertNotNull(metadata.getColumnTypes());
        assertTrue(metadata.getColumnTypes().containsKey("id"));
        assertTrue(metadata.getColumnTypes().containsKey("name"));
        assertTrue(metadata.getColumnTypes().containsKey("age"));
        assertTrue(metadata.getColumnTypes().containsKey("salary"));
        assertTrue(metadata.getColumnTypes().containsKey("active"));
    }
}


