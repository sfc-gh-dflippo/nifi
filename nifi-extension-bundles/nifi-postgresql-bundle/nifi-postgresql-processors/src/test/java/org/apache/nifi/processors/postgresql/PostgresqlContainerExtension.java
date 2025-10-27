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

import java.nio.file.Paths;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

/**
 * JUnit 5 extension that manages the lifecycle of a PostgreSQL Testcontainer with pg_parquet extension. This extension builds a custom PostgreSQL
 * Docker image with pg_parquet extension from a Dockerfile and starts the container before all tests in a class, stopping it after all tests
 * complete.
 *
 * The custom image includes: - PostgreSQL 17.2 - pg_parquet extension for reading/writing Parquet files
 *
 * <p>
 * Example usage:
 * </p>
 *
 * <pre>
 * &#64;ExtendWith(PostgresqlContainerExtension.class)
 * class MyIntegrationTest {
 *     &#64;Test
 *     void testDatabaseConnection() throws SQLException {
 *         String jdbcUrl = PostgresqlContainerExtension.getJdbcUrl();
 *         String username = PostgresqlContainerExtension.getUsername();
 *         String password = PostgresqlContainerExtension.getPassword();
 *
 *         try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
 *             // Test database operations including Parquet format
 *             // pg_parquet extension is automatically available
 *         }
 *     }
 * }
 * </pre>
 */
public class PostgresqlContainerExtension implements BeforeAllCallback, AfterAllCallback {

    // Use a custom PostgreSQL image with pg_parquet extension built from Dockerfile
    private static PostgreSQLContainer<?> postgresContainer;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (postgresContainer == null) {
            // Build custom PostgreSQL image with pg_parquet extension from Dockerfile
            new ImageFromDockerfile().withDockerfile(Paths.get("src/test/resources/docker/postgresql-pgparquet/Dockerfile"));

            DockerImageName customImageName = DockerImageName.parse("nifi-postgres-parquet:test").asCompatibleSubstituteFor("postgres");
            // Let Testcontainers use its default values for database name, username, and
            // password
            postgresContainer = new PostgreSQLContainer<>(customImageName).withReuse(true); // Reuse container across test classes for better
                                                                                            // performance

            postgresContainer.start();

            // Log container information for debugging (credentials excluded for security)
            System.out.println("PostgreSQL Testcontainer started:");
            System.out.println("  JDBC URL: " + maskJdbcUrl(postgresContainer.getJdbcUrl()));
            System.out.println("  Username: " + postgresContainer.getUsername());
            System.out.println("  Note: Password not logged for security");
            System.out.println("  Database: " + postgresContainer.getDatabaseName());
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        // Container will be stopped automatically when JVM exits due to Testcontainers
        // cleanup
        // We don't explicitly stop it here to allow container reuse across test classes
    }

    /**
     * Masks any credentials in the JDBC URL for safe logging.
     * Replaces password values with asterisks.
     * 
     * @param jdbcUrl the JDBC URL to mask
     * @return masked JDBC URL safe for logging
     */
    private static String maskJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        // Mask password in JDBC URLs like: jdbc:postgresql://host:port/db?user=xxx&password=yyy
        return jdbcUrl.replaceAll("([?&]password=)[^&]*", "$1****");
    }

    /**
     * Get the JDBC URL for the PostgreSQL container.
     *
     * @return JDBC URL string
     * @throws IllegalStateException
     *             if container is not started
     */
    public static String getJdbcUrl() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("PostgreSQL container is not running. Ensure the extension is properly configured.");
        }
        return postgresContainer.getJdbcUrl();
    }

    /**
     * Get the username for the PostgreSQL container.
     *
     * @return username string
     * @throws IllegalStateException
     *             if container is not started
     */
    public static String getUsername() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("PostgreSQL container is not running. Ensure the extension is properly configured.");
        }
        return postgresContainer.getUsername();
    }

    /**
     * Get the password for the PostgreSQL container.
     *
     * @return password string
     * @throws IllegalStateException
     *             if container is not started
     */
    public static String getPassword() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("PostgreSQL container is not running. Ensure the extension is properly configured.");
        }
        return postgresContainer.getPassword();
    }

    /**
     * Get the database name for the PostgreSQL container.
     *
     * @return database name string
     * @throws IllegalStateException
     *             if container is not started
     */
    public static String getDatabaseName() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("PostgreSQL container is not running. Ensure the extension is properly configured.");
        }
        return postgresContainer.getDatabaseName();
    }

    /**
     * Get the mapped port for the PostgreSQL container.
     *
     * @return mapped port number
     * @throws IllegalStateException
     *             if container is not started
     */
    public static Integer getMappedPort() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("PostgreSQL container is not running. Ensure the extension is properly configured.");
        }
        return postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }

    /**
     * Get the host for the PostgreSQL container.
     *
     * @return host string
     * @throws IllegalStateException
     *             if container is not started
     */
    public static String getHost() {
        if (postgresContainer == null || !postgresContainer.isRunning()) {
            throw new IllegalStateException("PostgreSQL container is not running. Ensure the extension is properly configured.");
        }
        return postgresContainer.getHost();
    }
}
