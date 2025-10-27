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

import org.apache.nifi.processor.Processor;
import org.apache.nifi.postgresql.service.PostgreSQLConnectionPool;
import org.apache.nifi.postgresql.service.util.CredentialManager.PostgreSQLCredentials;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.sql.Connection;

/**
 * Helper class for setting up PostgreSQL connection services in integration tests.
 * Provides consistent connection management across all test modules.
 * 
 * Usage:
 * <pre>
 * PostgreSQLTestHelper helper = PostgreSQLTestHelper.builder()
 *     .build();
 * 
 * &#64;BeforeEach
 * void setup() throws Exception {
 *     helper.setup();
 *     connection = helper.getConnection();
 *     connectionService = helper.getConnectionService();
 * }
 * 
 * &#64;AfterEach
 * void teardown() throws Exception {
 *     helper.teardown();
 * }
 * </pre>
 */
public class PostgreSQLTestHelper {
    
    private final Class<? extends Processor> testProcessorClass;
    private PostgreSQLConnectionProviderService connectionService;
    private TestRunner testRunner;
    private Connection connection;
    
    private PostgreSQLTestHelper(Class<? extends Processor> testProcessorClass) {
        this.testProcessorClass = testProcessorClass;
    }
    
    /**
     * Creates a builder for PostgreSQLTestHelper.
     * 
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Sets up the PostgreSQL connection service and test runner.
     * Call this in your &#64;BeforeEach method.
     * 
     * @throws Exception if setup fails
     */
    public void setup() throws Exception {
        PostgreSQLCredentials pgCreds = CredentialManager.getPostgreSQLCredentials();
        
        testRunner = TestRunners.newTestRunner(testProcessorClass);
        testRunner.setValidateExpressionUsage(false);
        
        connectionService = new PostgreSQLConnectionPool();
        testRunner.addControllerService("postgresql-service", connectionService);
        setConnectionProperties(connectionService, pgCreds);
        
        testRunner.assertValid(connectionService);
        testRunner.enableControllerService(connectionService);
        
        connection = connectionService.getPostgreSQLConnection().getConnection();
        connection.setAutoCommit(false);
    }
    
    /**
     * Cleans up resources. Call this in your &#64;AfterEach method.
     * 
     * @throws Exception if cleanup fails
     */
    public void teardown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.rollback();
            connection.close();
        }
    }
    
    /**
     * Gets the connection for test setup and SQL execution.
     * 
     * @return the database connection
     */
    public Connection getConnection() {
        return connection;
    }
    
    /**
     * Gets the connection service for fetching metadata.
     * 
     * @return the PostgreSQL connection provider service
     */
    public PostgreSQLConnectionProviderService getConnectionService() {
        return connectionService;
    }
    
    /**
     * Gets the test runner if needed for advanced test scenarios.
     * 
     * @return the test runner
     */
    public TestRunner getTestRunner() {
        return testRunner;
    }
    
    /**
     * Configures the connection service with database credentials.
     */
    private void setConnectionProperties(PostgreSQLConnectionProviderService service, PostgreSQLCredentials creds) throws Exception {
        Class<?> serviceClass = service.getClass();
        
        Object urlProperty = serviceClass.getField("POSTGRESQL_URL").get(null);
        Object userProperty = serviceClass.getField("POSTGRESQL_USER").get(null);
        Object passwordProperty = serviceClass.getField("POSTGRESQL_PASSWORD").get(null);
        
        testRunner.setProperty(service, (org.apache.nifi.components.PropertyDescriptor) urlProperty, creds.getJdbcUrl());
        testRunner.setProperty(service, (org.apache.nifi.components.PropertyDescriptor) userProperty, creds.getUserName());
        testRunner.setProperty(service, (org.apache.nifi.components.PropertyDescriptor) passwordProperty, creds.getPassword());
    }
    
    /**
     * Builder for PostgreSQLTestHelper.
     */
    public static class Builder {
        private Class<? extends Processor> testProcessorClass = org.apache.nifi.processors.standard.LogAttribute.class;
        
        /**
         * Sets the processor class to use for the TestRunner.
         * Defaults to LogAttribute if not specified.
         * 
         * @param processorClass the processor class
         * @return this builder
         */
        public Builder withProcessor(Class<? extends Processor> processorClass) {
            this.testProcessorClass = processorClass;
            return this;
        }
        
        /**
         * Builds the PostgreSQLTestHelper instance.
         * 
         * @return a new PostgreSQLTestHelper
         */
        public PostgreSQLTestHelper build() {
            return new PostgreSQLTestHelper(testProcessorClass);
        }
    }
}

