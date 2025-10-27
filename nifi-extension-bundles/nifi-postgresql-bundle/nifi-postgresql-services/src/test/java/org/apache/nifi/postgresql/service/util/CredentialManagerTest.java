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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Test class for CredentialManager functionality.
 * Tests credential resolution from various sources including config files and environment variables.
 */
public class CredentialManagerTest {

    private static final String USER_HOME = System.getProperty("user.home");
    private static final Path PG_SERVICE_FILE = Paths.get(USER_HOME, ".pg_service.conf");
    private static final Path SNOWFLAKE_CONNECTIONS_FILE = Paths.get(USER_HOME, ".snowflake", "connections.toml");
    
    // Backup original files if they exist
    private Path pgServiceBackup;
    private Path snowflakeConnectionsBackup;
    private boolean pgServiceExisted;
    private boolean snowflakeConnectionsExisted;
    
    /**
     * Generate a random test value to avoid hardcoded credentials in tests.
     * Uses UUID for uniqueness and randomness.
     */
    private String generateRandomTestValue(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    @BeforeEach
    public void setUp() throws IOException {
        // Clear credential cache
        CredentialManager.clearCredentialCache();
        
        // Backup existing config files
        backupConfigFiles();
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Restore original config files
        restoreConfigFiles();
        
        // Clear credential cache
        CredentialManager.clearCredentialCache();
    }

    private void backupConfigFiles() throws IOException {
        // Backup .pg_service.conf if it exists
        if (Files.exists(PG_SERVICE_FILE)) {
            pgServiceExisted = true;
            pgServiceBackup = Paths.get(USER_HOME, ".pg_service.conf.test_backup");
            Files.copy(PG_SERVICE_FILE, pgServiceBackup);
        }
        
        // Backup .snowflake/connections.toml if it exists
        if (Files.exists(SNOWFLAKE_CONNECTIONS_FILE)) {
            snowflakeConnectionsExisted = true;
            snowflakeConnectionsBackup = Paths.get(USER_HOME, ".snowflake", "connections.toml.test_backup");
            Files.copy(SNOWFLAKE_CONNECTIONS_FILE, snowflakeConnectionsBackup);
        }
    }

    private void restoreConfigFiles() throws IOException {
        // Remove test files
        if (Files.exists(PG_SERVICE_FILE)) {
            Files.delete(PG_SERVICE_FILE);
        }
        if (Files.exists(SNOWFLAKE_CONNECTIONS_FILE)) {
            Files.delete(SNOWFLAKE_CONNECTIONS_FILE);
        }
        
        // Restore original files
        if (pgServiceExisted && pgServiceBackup != null && Files.exists(pgServiceBackup)) {
            Files.move(pgServiceBackup, PG_SERVICE_FILE);
        }
        if (snowflakeConnectionsExisted && snowflakeConnectionsBackup != null && Files.exists(snowflakeConnectionsBackup)) {
            Files.move(snowflakeConnectionsBackup, SNOWFLAKE_CONNECTIONS_FILE);
        }
    }

    @Test
    public void testCredentialManagerBasicFunctionality() {
        System.out.println("=== Testing CredentialManager Basic Functionality ===");
        
        // Test that we can get PostgreSQL credentials (should fall back to defaults)
        CredentialManager.PostgreSQLCredentials pgCreds = CredentialManager.getPostgreSQLCredentials();
        Assertions.assertNotNull(pgCreds, "PostgreSQL credentials should not be null");
        Assertions.assertNotNull(pgCreds.getHost(), "PostgreSQL host should not be null");
        Assertions.assertTrue(pgCreds.getPort() > 0, "PostgreSQL port should be positive");
        Assertions.assertNotNull(pgCreds.getDatabase(), "PostgreSQL database should not be null");
        Assertions.assertNotNull(pgCreds.getUserName(), "PostgreSQL username should not be null");
        Assertions.assertNotNull(pgCreds.getSslMode(), "PostgreSQL SSL mode should not be null");
        
        System.out.println("PostgreSQL Credentials: " + pgCreds);
        System.out.println("PostgreSQL JDBC URL: " + pgCreds.getJdbcUrl());
        
        // Test credential availability checks
        boolean hasPgCreds = CredentialManager.hasPostgreSQLCredentials();
        Assertions.assertTrue(hasPgCreds, "Should have PostgreSQL credentials");
        
        boolean hasSnowflakeCreds = CredentialManager.hasSnowflakeCredentials();
        System.out.println("Has Snowflake credentials: " + hasSnowflakeCreds);
        
        if (hasSnowflakeCreds) {
            CredentialManager.SnowflakeCredentials sfCreds = CredentialManager.getSnowflakeCredentials();
            Assertions.assertNotNull(sfCreds, "Snowflake credentials should not be null");
            Assertions.assertNotNull(sfCreds.getAccount(), "Snowflake account should not be null");
            Assertions.assertNotNull(sfCreds.getUser(), "Snowflake user should not be null");
            
            System.out.println("Snowflake Credentials: " + sfCreds);
            System.out.println("Snowflake JDBC URL: " + sfCreds.getJdbcUrl());
        } else {
            System.out.println("No Snowflake credentials available - this is expected if not configured");
        }
    }

    @Test
    public void testPostgreSQLCredentialsFromPgServiceFile() throws IOException {
        System.out.println("=== Testing PostgreSQL Credentials from .pg_service.conf ===");
        
        // Generate random test values to avoid hardcoded credentials
        String testHost = generateRandomTestValue("host");
        String testDatabase = generateRandomTestValue("db");
        String testUser = generateRandomTestValue("user");
        String testPassword = generateRandomTestValue("pass");
        String secondaryHost = generateRandomTestValue("host");
        String secondaryUser = generateRandomTestValue("user");
        String secondaryPassword = generateRandomTestValue("pass");
        
        // Create a test .pg_service.conf file
        String pgServiceContent = 
            "[default]\n" +
            "host=" + testHost + "\n" +
            "port=5433\n" +
            "dbname=" + testDatabase + "\n" +
            "user=" + testUser + "\n" +
            "password=" + testPassword + "\n" +
            "sslmode=require\n" +
            "\n" +
            "[secondary]\n" +
            "host=" + secondaryHost + "\n" +
            "port=5434\n" +
            "user=" + secondaryUser + "\n" +
            "password=" + secondaryPassword + "\n";
        
        Files.write(PG_SERVICE_FILE, pgServiceContent.getBytes());
        
        // Clear cache to force re-reading
        CredentialManager.clearCredentialCache();
        
        // Test reading credentials
        CredentialManager.PostgreSQLCredentials creds = CredentialManager.getPostgreSQLCredentials();
        
        Assertions.assertEquals(testHost, creds.getHost());
        Assertions.assertEquals(5433, creds.getPort());
        Assertions.assertEquals(testDatabase, creds.getDatabase());
        Assertions.assertEquals(testUser, creds.getUserName());
        Assertions.assertEquals(testPassword, creds.getPassword());
        Assertions.assertEquals("require", creds.getSslMode());
        
        String expectedJdbcUrl = "jdbc:postgresql://" + testHost + ":5433/" + testDatabase + "?sslmode=require";
        Assertions.assertEquals(expectedJdbcUrl, creds.getJdbcUrl());
        
        System.out.println("Successfully read PostgreSQL credentials from .pg_service.conf");
        System.out.println("Credentials: " + creds);
    }

    @Test
    public void testSnowflakeCredentialsFromConnectionsToml() throws IOException {
        System.out.println("=== Testing Snowflake Credentials from connections.toml ===");
        
        // Ensure .snowflake directory exists
        Path snowflakeDir = SNOWFLAKE_CONNECTIONS_FILE.getParent();
        if (!Files.exists(snowflakeDir)) {
            Files.createDirectories(snowflakeDir);
        }
        
        // Generate random test values to avoid hardcoded credentials
        String testAccount = generateRandomTestValue("account");
        String testUser = generateRandomTestValue("user");
        String testPassword = generateRandomTestValue("pass");
        String testWarehouse = generateRandomTestValue("wh").toUpperCase();
        String testDatabase = generateRandomTestValue("db").toUpperCase();
        String testSchema = generateRandomTestValue("schema").toUpperCase();
        String testRole = generateRandomTestValue("role").toUpperCase();
        String devAccount = generateRandomTestValue("account");
        String devUser = generateRandomTestValue("user");
        String keyPassword = generateRandomTestValue("keypwd");
        
        // Create a test connections.toml file
        String connectionsContent = 
            "[default]\n" +
            "account = \"" + testAccount + "\"\n" +
            "user = \"" + testUser + "\"\n" +
            "password = \"" + testPassword + "\"\n" +
            "warehouse = \"" + testWarehouse + "\"\n" +
            "database = \"" + testDatabase + "\"\n" +
            "schema = \"" + testSchema + "\"\n" +
            "role = \"" + testRole + "\"\n" +
            "\n" +
            "[dev]\n" +
            "account = \"" + devAccount + "\"\n" +
            "user = \"" + devUser + "\"\n" +
            "private_key_file = \"/path/to/key.p8\"\n" +
            "private_key_file_pwd = \"" + keyPassword + "\"\n";
        
        Files.write(SNOWFLAKE_CONNECTIONS_FILE, connectionsContent.getBytes());
        
        // Clear cache to force re-reading
        CredentialManager.clearCredentialCache();
        
        // Test reading credentials
        CredentialManager.SnowflakeCredentials creds = CredentialManager.getSnowflakeCredentials();
        
        Assertions.assertEquals(testAccount, creds.getAccount());
        Assertions.assertEquals(testUser, creds.getUser());
        Assertions.assertEquals(testPassword, creds.getPassword());
        Assertions.assertEquals(testWarehouse, creds.getWarehouse());
        Assertions.assertEquals(testDatabase, creds.getDatabase());
        Assertions.assertEquals(testSchema, creds.getSchema());
        Assertions.assertEquals(testRole, creds.getRole());
        
        String expectedJdbcUrl = "jdbc:snowflake://" + testAccount + ".snowflakecomputing.com";
        Assertions.assertEquals(expectedJdbcUrl, creds.getJdbcUrl());
        
        System.out.println("Successfully read Snowflake credentials from connections.toml");
        System.out.println("Credentials: " + creds);
    }

    @Test
    public void testCredentialCaching() {
        System.out.println("=== Testing Credential Caching ===");
        
        // Get credentials twice and verify they're the same object (cached)
        CredentialManager.PostgreSQLCredentials creds1 = CredentialManager.getPostgreSQLCredentials();
        CredentialManager.PostgreSQLCredentials creds2 = CredentialManager.getPostgreSQLCredentials();
        
        Assertions.assertSame(creds1, creds2, "Credentials should be cached and return same object");
        
        // Clear cache and get again - should be different object
        CredentialManager.clearCredentialCache();
        CredentialManager.PostgreSQLCredentials creds3 = CredentialManager.getPostgreSQLCredentials();
        
        Assertions.assertNotSame(creds1, creds3, "After cache clear, should get new object");
        
        // But values should be the same
        Assertions.assertEquals(creds1.getHost(), creds3.getHost());
        Assertions.assertEquals(creds1.getPort(), creds3.getPort());
        Assertions.assertEquals(creds1.getDatabase(), creds3.getDatabase());
        
        System.out.println("Credential caching works correctly");
    }

    @Test
    public void testCredentialAvailabilityChecks() {
        System.out.println("=== Testing Credential Availability Checks ===");
        
        // PostgreSQL should always be available (falls back to defaults)
        boolean hasPg = CredentialManager.hasPostgreSQLCredentials();
        Assertions.assertTrue(hasPg, "PostgreSQL credentials should always be available");
        
        // Snowflake availability depends on configuration
        boolean hasSnowflake = CredentialManager.hasSnowflakeCredentials();
        System.out.println("Snowflake credentials available: " + hasSnowflake);
        
        if (hasSnowflake) {
            // If available, should be able to get them without exception
            Assertions.assertDoesNotThrow(() -> {
                CredentialManager.SnowflakeCredentials creds = CredentialManager.getSnowflakeCredentials();
                Assertions.assertNotNull(creds);
            });
        } else {
            // If not available, should throw exception when trying to get them
            Assertions.assertThrows(CredentialManager.CredentialException.class, () -> {
                CredentialManager.getSnowflakeCredentials();
            });
        }
        
        System.out.println("Credential availability checks work correctly");
    }

    @Test
    public void testJdbcUrlGeneration() {
        System.out.println("=== Testing JDBC URL Generation ===");
        
        // Test PostgreSQL JDBC URL
        CredentialManager.PostgreSQLCredentials pgCreds = CredentialManager.getPostgreSQLCredentials();
        String pgJdbcUrl = pgCreds.getJdbcUrl();
        
        Assertions.assertNotNull(pgJdbcUrl, "PostgreSQL JDBC URL should not be null");
        Assertions.assertTrue(pgJdbcUrl.startsWith("jdbc:postgresql://"), "PostgreSQL JDBC URL should start with jdbc:postgresql://");
        Assertions.assertTrue(pgJdbcUrl.contains("sslmode="), "PostgreSQL JDBC URL should contain sslmode parameter");
        
        System.out.println("PostgreSQL JDBC URL: " + pgJdbcUrl);
        
        // Test Snowflake JDBC URL if available
        if (CredentialManager.hasSnowflakeCredentials()) {
            CredentialManager.SnowflakeCredentials sfCreds = CredentialManager.getSnowflakeCredentials();
            String sfJdbcUrl = sfCreds.getJdbcUrl();
            
            Assertions.assertNotNull(sfJdbcUrl, "Snowflake JDBC URL should not be null");
            Assertions.assertTrue(sfJdbcUrl.startsWith("jdbc:snowflake://"), "Snowflake JDBC URL should start with jdbc:snowflake://");
            Assertions.assertTrue(sfJdbcUrl.endsWith(".snowflakecomputing.com"), "Snowflake JDBC URL should end with .snowflakecomputing.com");
            
            System.out.println("Snowflake JDBC URL: " + sfJdbcUrl);
        }
        
        System.out.println("JDBC URL generation works correctly");
    }

    @Test
    public void testErrorHandling() {
        System.out.println("=== Testing Error Handling ===");
        
        // Test that missing Snowflake credentials throw appropriate exception
        if (!CredentialManager.hasSnowflakeCredentials()) {
            CredentialManager.CredentialException exception = Assertions.assertThrows(
                CredentialManager.CredentialException.class,
                () -> CredentialManager.getSnowflakeCredentials(),
                "Should throw CredentialException when Snowflake credentials are not available"
            );
            
            Assertions.assertTrue(exception.getMessage().contains("No Snowflake credentials found"),
                "Exception message should indicate Snowflake credentials not found");
            
            System.out.println("Error handling works correctly: " + exception.getMessage());
        } else {
            System.out.println("Skipping error handling test - Snowflake credentials are available");
        }
    }

    @Test
    public void testPrivateKeyReaderFunctionality() throws Exception {
        System.out.println("=== Testing PrivateKeyReader Functionality ===");
        
        // Test error handling for non-existent file
        Assertions.assertThrows(CredentialManager.CredentialException.class, () -> {
            CredentialManager.PrivateKeyReader.readPrivateKey("/non/existent/file.p8");
        }, "Should throw exception for non-existent file");
        
        // Test error handling for null filename
        Assertions.assertThrows(CredentialManager.CredentialException.class, () -> {
            CredentialManager.PrivateKeyReader.readPrivateKey(null);
        }, "Should throw exception for null filename");
        
        // Test error handling for empty filename
        Assertions.assertThrows(CredentialManager.CredentialException.class, () -> {
            CredentialManager.PrivateKeyReader.readPrivateKey("");
        }, "Should throw exception for empty filename");
        
        System.out.println("✓ PrivateKeyReader error handling works correctly");
    }

    @Test
    public void testSnowflakeCredentialsWithPrivateKeyObject() {
        System.out.println("=== Testing SnowflakeCredentials with PrivateKey Object ===");
        
        // Generate random test values to avoid hardcoded credentials
        String testAccount = generateRandomTestValue("account");
        String testUser = generateRandomTestValue("user");
        String testRole = generateRandomTestValue("role").toUpperCase();
        String testWarehouse = generateRandomTestValue("wh").toUpperCase();
        String testDatabase = generateRandomTestValue("db").toUpperCase();
        String testSchema = generateRandomTestValue("schema").toUpperCase();
        
        // Test constructor with PrivateKey object (using null for this test)
        CredentialManager.SnowflakeCredentials creds = new CredentialManager.SnowflakeCredentials(
            testAccount, testUser, null, (PrivateKey) null,
            testRole, testWarehouse, testDatabase, testSchema
        );
        
        Assertions.assertEquals(testAccount, creds.getAccount());
        Assertions.assertEquals(testUser, creds.getUser());
        Assertions.assertNull(creds.getPassword());
        Assertions.assertNull(creds.getPrivateKeyFile());
        Assertions.assertNull(creds.getPrivateKeyFilePassword());
        Assertions.assertNull(creds.getPrivateKey());
        Assertions.assertEquals(testRole, creds.getRole());
        
        // Test getResolvedPrivateKey with null private key
        PrivateKey resolvedKey = creds.getResolvedPrivateKey();
        Assertions.assertNull(resolvedKey, "Should return null when no private key is configured");
        
        // Test toString method shows correct auth method
        String toString = creds.toString();
        Assertions.assertTrue(toString.contains("auth='None'"), "Should show 'None' auth method when no auth is configured");
        
        System.out.println("SnowflakeCredentials with PrivateKey object works correctly");
        System.out.println("Credentials: " + creds);
    }

    @Test
    public void testConfigurationSummary() {
        System.out.println("\n=== Configuration Summary ===");
        
        // PostgreSQL Configuration
        System.out.println("PostgreSQL Configuration:");
        System.out.println("  .pg_service.conf exists: " + Files.exists(PG_SERVICE_FILE));
        
        CredentialManager.PostgreSQLCredentials pgCreds = CredentialManager.getPostgreSQLCredentials();
        System.out.println("  Host: " + pgCreds.getHost());
        System.out.println("  Port: " + pgCreds.getPort());
        System.out.println("  Database: " + pgCreds.getDatabase());
        System.out.println("  Username: " + pgCreds.getUserName());
        System.out.println("  SSL Mode: " + pgCreds.getSslMode());
        System.out.println("  JDBC URL: " + pgCreds.getJdbcUrl());
        
        // Snowflake Configuration
        System.out.println("\nSnowflake Configuration:");
        System.out.println("  connections.toml exists: " + Files.exists(SNOWFLAKE_CONNECTIONS_FILE));
        System.out.println("  Credentials available: " + CredentialManager.hasSnowflakeCredentials());
        
        if (CredentialManager.hasSnowflakeCredentials()) {
            CredentialManager.SnowflakeCredentials sfCreds = CredentialManager.getSnowflakeCredentials();
            System.out.println("  Account: " + sfCreds.getAccount());
            System.out.println("  User: " + sfCreds.getUser());
            System.out.println("  Warehouse: " + sfCreds.getWarehouse());
            System.out.println("  Database: " + sfCreds.getDatabase());
            System.out.println("  Schema: " + sfCreds.getSchema());
            System.out.println("  Role: " + sfCreds.getRole());
            System.out.println("  JDBC URL: " + sfCreds.getJdbcUrl());
            
            // Show detailed authentication method
            String authMethod = "None";
            if (sfCreds.getPassword() != null && !sfCreds.getPassword().isEmpty()) {
                authMethod = "Password";
            } else if (sfCreds.getPrivateKey() != null) {
                authMethod = "Private Key (Object)";
            } else if (sfCreds.getPrivateKeyFile() != null && !sfCreds.getPrivateKeyFile().isEmpty()) {
                authMethod = "Private Key (File: " + sfCreds.getPrivateKeyFile() + ")";
            }
            System.out.println("  Auth Method: " + authMethod);
        } else {
            System.out.println("  To enable Snowflake tests:");
            System.out.println("    1. Create ~/.snowflake/connections.toml with [default] connection");
            System.out.println("    2. Or set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER environment variables");
        }
        
        // Environment Variables
        System.out.println("\nEnvironment Variables:");
        String[] pgEnvVars = {"PGHOST", "PGUSER", "POSTGRESQL_HOST", "POSTGRESQL_USERNAME"};
        for (String var : pgEnvVars) {
            String value = System.getenv(var);
            System.out.println("  " + var + ": " + (value != null ? "SET" : "NOT SET"));
        }
        
        String[] sfEnvVars = {"SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"};
        for (String var : sfEnvVars) {
            String value = System.getenv(var);
            System.out.println("  " + var + ": " + (value != null ? "SET" : "NOT SET"));
        }
        
        System.out.println("\n=== End Configuration Summary ===");
    }

    @Test
    public void testPostgreSQLConnectivity() {
        System.out.println("=== Testing PostgreSQL Connectivity ===");
        
        try {
            CredentialManager.PostgreSQLCredentials creds = CredentialManager.getPostgreSQLCredentials();
            System.out.println("Attempting to connect to PostgreSQL...");
            System.out.println("Host: " + creds.getHost());
            System.out.println("Port: " + creds.getPort());
            System.out.println("Database: " + creds.getDatabase());
            System.out.println("Username: " + creds.getUserName());
            
            // Test actual database connectivity
            boolean connected = testPostgreSQLConnection(creds);
            
            if (connected) {
                System.out.println("✅ PostgreSQL connectivity test PASSED");
            } else {
                System.out.println("❌ PostgreSQL connectivity test FAILED - connection unsuccessful");
            }
            
            // Don't fail the test if connection fails - credentials might be valid but DB unavailable
            System.out.println("PostgreSQL connectivity test completed");
            
        } catch (Exception e) {
            System.out.println("❌ PostgreSQL connectivity test FAILED with exception: " + e.getMessage());
            // Don't fail the test - this might be expected in some environments
        }
    }

    @Test
    public void testSnowflakeConnectivity() {
        System.out.println("=== Testing Snowflake Connectivity ===");
        
        if (!CredentialManager.hasSnowflakeCredentials()) {
            System.out.println("⏭️  Skipping Snowflake connectivity test - no credentials available");
            return;
        }
        
        try {
            CredentialManager.SnowflakeCredentials creds = CredentialManager.getSnowflakeCredentials();
            System.out.println("Attempting to connect to Snowflake...");
            System.out.println("Account: " + creds.getAccount());
            System.out.println("User: " + creds.getUser());
            System.out.println("Warehouse: " + creds.getWarehouse());
            System.out.println("Database: " + creds.getDatabase());
            System.out.println("Schema: " + creds.getSchema());
            
            // Test actual database connectivity
            boolean connected = testSnowflakeConnection(creds);
            
            if (connected) {
                System.out.println("✅ Snowflake connectivity test PASSED");
            } else {
                System.out.println("❌ Snowflake connectivity test FAILED - connection unsuccessful");
            }
            
            // Don't fail the test if connection fails - credentials might be valid but DB unavailable
            System.out.println("Snowflake connectivity test completed");
            
        } catch (Exception e) {
            System.out.println("❌ Snowflake connectivity test FAILED with exception: " + e.getMessage());
            // Don't fail the test - this might be expected in some environments
        }
    }

    /**
     * Test PostgreSQL connection with actual database connectivity.
     */
    private boolean testPostgreSQLConnection(CredentialManager.PostgreSQLCredentials creds) {
        try {
            // Load PostgreSQL driver
            Class.forName("org.postgresql.Driver");
            
            // Create connection properties
            Properties props = new Properties();
            props.setProperty("user", creds.getUserName());
            if (creds.getPassword() != null && !creds.getPassword().isEmpty()) {
                props.setProperty("password", creds.getPassword());
            }
            props.setProperty("sslmode", creds.getSslMode());
            
            // Test connection with a short timeout
            props.setProperty("connectTimeout", "10"); // 10 seconds
            props.setProperty("socketTimeout", "10");   // 10 seconds
            
            System.out.println("Connecting with JDBC URL: " + creds.getJdbcUrl());
            
            try (Connection conn = DriverManager.getConnection(creds.getJdbcUrl(), props)) {
                // Test with a simple query
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT 1 as test_connection")) {
                        if (rs.next()) {
                            int result = rs.getInt("test_connection");
                            System.out.println("PostgreSQL query result: " + result);
                            return result == 1;
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            System.out.println("PostgreSQL connection failed: " + e.getMessage());
            return false;
        }
        
        return false;
    }

    /**
     * Test Snowflake connection with actual database connectivity.
     */
    private boolean testSnowflakeConnection(CredentialManager.SnowflakeCredentials creds) {
        try {
            // Load Snowflake driver
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            
            // Create connection properties
            Properties props = new Properties();
            props.setProperty("user", creds.getUser());
            
            // Handle authentication method
            if (creds.getPassword() != null && !creds.getPassword().isEmpty()) {
                props.setProperty("password", creds.getPassword());
            } else {
                // Try to get resolved private key (handles both file and object cases)
                try {
                    PrivateKey privateKey = creds.getResolvedPrivateKey();
                    if (privateKey != null) {
                        props.put("privateKey", privateKey);
                    }
                } catch (Exception e) {
                    System.out.println("Warning: Failed to load private key: " + e.getMessage());
                    // Fall back to file-based approach if available
                    if (creds.getPrivateKeyFile() != null) {
                        props.setProperty("private_key_file", creds.getPrivateKeyFile());
                        if (creds.getPrivateKeyFilePassword() != null) {
                            props.setProperty("private_key_file_pwd", creds.getPrivateKeyFilePassword());
                        }
                    }
                }
            }
            
            // Set Snowflake-specific properties
            if (creds.getWarehouse() != null) {
                props.setProperty("warehouse", creds.getWarehouse());
            }
            if (creds.getDatabase() != null) {
                props.setProperty("db", creds.getDatabase());
            }
            if (creds.getSchema() != null) {
                props.setProperty("schema", creds.getSchema());
            }
            if (creds.getRole() != null) {
                props.setProperty("role", creds.getRole());
            }
            
            // Connection timeout settings
            props.setProperty("loginTimeout", "30");
            props.setProperty("networkTimeout", "30000");
            
            System.out.println("Connecting with JDBC URL: " + creds.getJdbcUrl());
            
            try (Connection conn = DriverManager.getConnection(creds.getJdbcUrl(), props)) {
                // Test with a simple query
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT 1 as test_connection")) {
                        if (rs.next()) {
                            int result = rs.getInt("test_connection");
                            System.out.println("Snowflake query result: " + result);
                            
                            // Also test current context
                            try (ResultSet contextRs = stmt.executeQuery("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")) {
                                if (contextRs.next()) {
                                    System.out.println("Current Warehouse: " + contextRs.getString(1));
                                    System.out.println("Current Database: " + contextRs.getString(2));
                                    System.out.println("Current Schema: " + contextRs.getString(3));
                                    System.out.println("Current Role: " + contextRs.getString(4));
                                }
                            }
                            
                            return result == 1;
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            System.out.println("Snowflake connection failed: " + e.getMessage());
            return false;
        }
        
        return false;
    }
}
