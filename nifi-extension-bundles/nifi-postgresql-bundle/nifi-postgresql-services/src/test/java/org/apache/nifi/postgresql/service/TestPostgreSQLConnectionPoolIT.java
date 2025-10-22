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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.postgresql.service.util.ConnectionPoolSettings;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionWrapper;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager;
import org.apache.nifi.processors.postgresql.integration.credentials.CredentialManager.PostgreSQLCredentials;
import org.apache.nifi.processors.postgresql.util.ConnectionSettings;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.postgresql.PGConnection;

/**
 * Integration test covering connection pool configuration and a simple PG unwrap.
 * Uses CredentialManager to load credentials from ~/.pg_service.conf or environment variables.
 */
public class TestPostgreSQLConnectionPoolIT {

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
        runner.addControllerService("postgresqlConnectionProviderService", connectionProviderService);

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
}


