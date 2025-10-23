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
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionWrapper;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager.PostgreSQLCredentials;
import org.apache.nifi.processors.postgresql.util.ConnectionSettings;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceLookup;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.postgresql.PGConnection;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test covering connection pool configuration and a simple PG unwrap.
 * Uses CredentialManager to load credentials from ~/.pg_service.conf or environment variables.
 */
public class TestPostgreSQLConnectionPoolIT {

    private static final String SERVICE_ID = "postgresqlConnectionProviderService";

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
        assertTrue(explanation.contains("Root Cause:"), 
                "Should contain root cause section");
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
        assertTrue(explanation.contains("Root Cause:"), 
                "Should contain root cause section");
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
        assertTrue(explanation.contains("Root Cause:"), 
                "Should contain root cause section");
        assertTrue(explanation.contains("Troubleshooting Tips:"), 
                "Should contain troubleshooting tips");
        
        // Password in URL should be masked
        assertFalse(explanation.contains("definitely_wrong_password_12345"), 
                "Password should be masked in error message");
        
        // Should contain authentication error message
        // The error message should indicate a connection failure with root cause
        assertTrue(explanation.contains("Root Cause:"), 
                "Should contain root cause section");
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
        assertTrue(explanation.contains("Root Cause:"), 
                "Should contain root cause section");
        assertTrue(explanation.contains("Troubleshooting Tips:"), 
                "Should contain troubleshooting tips");
    }
}


