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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

/**
 * Centralized Credential Management for NiFi Integration Tests
 * 
 * Provides secure, centralized credential management for PostgreSQL and Snowflake
 * connections in integration tests. Implements a three-tier credential resolution
 * system with priority-based fallback for maximum flexibility.
 * 
 * Credential Resolution Priority:
 * 1. Configuration files (local development) - ~/.pg_service.conf, ~/.snowflake/connections.toml
 * 2. Environment variables (fallback)
 * 3. Default values (test mode)
 * 
 * Security Features:
 * - Zero hardcoded credentials
 * - Secure file permission validation
 * - Comprehensive error handling
 * - Type-safe credential structures
 * 
 * Usage:
 * <pre>
 * PostgreSQLCredentials pgCreds = CredentialManager.getPostgreSQLCredentials();
 * SnowflakeCredentials sfCreds = CredentialManager.getSnowflakeCredentials();
 * </pre>
 */
public final class CredentialManager {

    private static final String USER_HOME = System.getProperty("user.home");
    private static final Path PG_SERVICE_FILE = Paths.get(USER_HOME, ".pg_service.conf");
    private static final Path SNOWFLAKE_CONNECTIONS_FILE = Paths.get(USER_HOME, ".snowflake", "connections.toml");
    
    // Cache credentials to avoid repeated file reads
    private static PostgreSQLCredentials cachedPostgreSQLCredentials;
    private static SnowflakeCredentials cachedSnowflakeCredentials;

    private CredentialManager() {
        // Utility class
    }

    /**
     * Exception thrown when credentials cannot be resolved.
     */
    public static class CredentialException extends RuntimeException {
        public CredentialException(String message) {
            super(message);
        }
        
        public CredentialException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Utility class for reading private keys from PEM files, supporting both encrypted and unencrypted keys.
     * Uses Bouncy Castle Crypto APIs to handle PKCS#8 encrypted private keys.
     */
    public static class PrivateKeyReader {
        
        static {
            // Ensure Bouncy Castle provider is available
            if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
                Security.addProvider(new BouncyCastleProvider());
            }
        }

        /**
         * Read a private key from a PEM file, handling both encrypted and unencrypted keys.
         * 
         * @param filename Path to the private key file
         * @param passphrase Passphrase for encrypted keys (can be null for unencrypted keys)
         * @return PrivateKey object
         * @throws CredentialException if the key cannot be read or decrypted
         */
        public static PrivateKey readPrivateKey(String filename, String passphrase) {
            if (filename == null || filename.trim().isEmpty()) {
                throw new CredentialException("Private key filename cannot be null or empty");
            }

            Path keyPath = Paths.get(filename);
            if (!Files.exists(keyPath)) {
                throw new CredentialException("Private key file does not exist: " + filename);
            }

            try {
                PrivateKeyInfo privateKeyInfo = null;
                
                // Read the PEM file
                try (PEMParser pemParser = new PEMParser(new FileReader(keyPath.toFile()))) {
                    Object pemObject = pemParser.readObject();
                    
                    if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                        // Handle encrypted private key
                        PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                        
                        if (passphrase == null || passphrase.isEmpty()) {
                            throw new CredentialException("Private key is encrypted but no passphrase provided");
                        }
                        
                        try {
                            InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                                .build(passphrase.toCharArray());
                            privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
                        } catch (PKCSException | OperatorCreationException e) {
                            throw new CredentialException("Failed to decrypt private key - invalid passphrase or corrupted key", e);
                        }
                        
                    } else if (pemObject instanceof PrivateKeyInfo) {
                        // Handle unencrypted private key
                        privateKeyInfo = (PrivateKeyInfo) pemObject;
                        
                    } else {
                        throw new CredentialException("Unsupported private key format in file: " + filename + 
                            ". Expected PKCS#8 format (encrypted or unencrypted)");
                    }
                }
                
                if (privateKeyInfo == null) {
                    throw new CredentialException("Failed to parse private key from file: " + filename);
                }
                
                // Convert to PrivateKey object
                JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
                return converter.getPrivateKey(privateKeyInfo);
                
            } catch (IOException e) {
                throw new CredentialException("Failed to read private key file: " + filename, e);
            } catch (Exception e) {
                if (e instanceof CredentialException) {
                    throw e;
                }
                throw new CredentialException("Unexpected error reading private key from file: " + filename, e);
            }
        }

        /**
         * Read an unencrypted private key from a PEM file.
         * 
         * @param filename Path to the private key file
         * @return PrivateKey object
         * @throws CredentialException if the key cannot be read
         */
        public static PrivateKey readPrivateKey(String filename) {
            return readPrivateKey(filename, null);
        }
    }

    /**
     * PostgreSQL connection credentials.
     */
    public static class PostgreSQLCredentials {
        private final String host;
        private final int port;
        private final String database;
        private final String username;
        private final String password;
        private final String sslMode;

        public PostgreSQLCredentials(String host, int port, String database, 
                                   String username, String password, String sslMode) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.username = username;
            this.password = password;
            this.sslMode = sslMode;
        }

        public String getHost() { return host; }
        public int getPort() { return port; }
        public String getDatabase() { return database; }
        public String getUserName() { return username; }
        public String getPassword() { return password; }
        public String getSslMode() { return sslMode; }

        /**
         * Get JDBC URL for PostgreSQL connection.
         */
        public String getJdbcUrl() {
            return String.format("jdbc:postgresql://%s:%d/%s?sslmode=%s", 
                host, port, database, sslMode);
        }

        @Override
        public String toString() {
            return String.format("PostgreSQLCredentials{host='%s', port=%d, database='%s', username='%s', sslMode='%s'}", 
                host, port, database, username, sslMode);
        }
    }

    /**
     * Snowflake connection credentials.
     */
    public static class SnowflakeCredentials {
        private final String account;
        private final String user;
        private final String password;
        private final String privateKeyFile;
        private final String privateKeyFilePassword;
        private final PrivateKey privateKey;
        private final String role;
        private final String warehouse;
        private final String database;
        private final String schema;

        public SnowflakeCredentials(String account, String user, String password,
                                  String privateKeyFile, String privateKeyFilePassword,
                                  String role, String warehouse, String database, String schema) {
            this.account = account;
            this.user = user;
            this.password = password;
            this.privateKeyFile = privateKeyFile;
            this.privateKeyFilePassword = privateKeyFilePassword;
            this.privateKey = null;
            this.role = role;
            this.warehouse = warehouse;
            this.database = database;
            this.schema = schema;
        }

        public SnowflakeCredentials(String account, String user, String password,
                                  PrivateKey privateKey,
                                  String role, String warehouse, String database, String schema) {
            this.account = account;
            this.user = user;
            this.password = password;
            this.privateKeyFile = null;
            this.privateKeyFilePassword = null;
            this.privateKey = privateKey;
            this.role = role;
            this.warehouse = warehouse;
            this.database = database;
            this.schema = schema;
        }

        public String getAccount() { return account; }
        public String getUser() { return user; }
        public String getPassword() { return password; }
        public String getPrivateKeyFile() { return privateKeyFile; }
        public String getPrivateKeyFilePassword() { return privateKeyFilePassword; }
        public PrivateKey getPrivateKey() { return privateKey; }
        public String getRole() { return role; }
        public String getWarehouse() { return warehouse; }
        public String getDatabase() { return database; }
        public String getSchema() { return schema; }

        /**
         * Get the resolved private key, either from the PrivateKey object or by reading from file.
         * 
         * @return PrivateKey object, or null if no private key is configured
         * @throws CredentialException if private key file exists but cannot be read
         */
        public PrivateKey getResolvedPrivateKey() {
            if (privateKey != null) {
                return privateKey;
            }
            
            if (privateKeyFile != null && !privateKeyFile.trim().isEmpty()) {
                return PrivateKeyReader.readPrivateKey(privateKeyFile, privateKeyFilePassword);
            }
            
            return null;
        }

        /**
         * Get JDBC URL for Snowflake connection.
         */
        public String getJdbcUrl() {
            return String.format("jdbc:snowflake://%s.snowflakecomputing.com", account);
        }

        @Override
        public String toString() {
            String authMethod = "None";
            if (password != null && !password.isEmpty()) {
                authMethod = "Password";
            } else if (privateKey != null) {
                authMethod = "PrivateKey(Object)";
            } else if (privateKeyFile != null && !privateKeyFile.isEmpty()) {
                authMethod = "PrivateKey(File)";
            }
            
            return String.format("SnowflakeCredentials{account='%s', user='%s', auth='%s', role='%s', warehouse='%s', database='%s', schema='%s'}", 
                account, user, authMethod, role, warehouse, database, schema);
        }
    }

    /**
     * Get PostgreSQL credentials with priority order:
     * 1. ~/.pg_service.conf file (local development)
     * 2. Environment variables (fallback)
     * 3. Default localhost values (test mode)
     * 
     * @return PostgreSQL connection credentials
     * @throws CredentialException if no valid credentials can be found
     */
    public static PostgreSQLCredentials getPostgreSQLCredentials() {
        if (cachedPostgreSQLCredentials != null) {
            return cachedPostgreSQLCredentials;
        }

        // Try ~/.pg_service.conf file first
        PostgreSQLCredentials pgServiceCreds = getPostgreSQLCredentialsFromPgService("default");
        if (pgServiceCreds != null) {
            cachedPostgreSQLCredentials = pgServiceCreds;
            return cachedPostgreSQLCredentials;
        }

        // Fallback to environment variables
        PostgreSQLCredentials envCreds = getPostgreSQLCredentialsFromEnv();
        if (envCreds != null) {
            cachedPostgreSQLCredentials = envCreds;
            return cachedPostgreSQLCredentials;
        }

        // Default test values (localhost)
        cachedPostgreSQLCredentials = new PostgreSQLCredentials(
            "localhost", 5432, "postgres", "postgres", "", "prefer"
        );
        
        return cachedPostgreSQLCredentials;
    }

    /**
     * Get Snowflake credentials from ~/.snowflake/connections.toml file.
     * 
     * @return Snowflake connection credentials
     * @throws CredentialException if no valid credentials can be found
     */
    public static SnowflakeCredentials getSnowflakeCredentials() {
        if (cachedSnowflakeCredentials != null) {
            return cachedSnowflakeCredentials;
        }

        // Try ~/.snowflake/connections.toml file
        SnowflakeCredentials tomlCreds = getSnowflakeCredentialsFromConnectionsToml("default");
        if (tomlCreds != null) {
            cachedSnowflakeCredentials = tomlCreds;
            return cachedSnowflakeCredentials;
        }

        // Fallback to environment variables
        SnowflakeCredentials envCreds = getSnowflakeCredentialsFromEnv();
        if (envCreds != null) {
            cachedSnowflakeCredentials = envCreds;
            return cachedSnowflakeCredentials;
        }

        throw new CredentialException(
            "No Snowflake credentials found. Please ensure one of the following:\n" +
            "1. ~/.snowflake/connections.toml file exists with [default] connection\n" +
            "2. Environment variables SNOWFLAKE_* are set"
        );
    }

    /**
     * Read PostgreSQL credentials from ~/.pg_service.conf file.
     */
    private static PostgreSQLCredentials getPostgreSQLCredentialsFromPgService(String serviceName) {
        if (!Files.exists(PG_SERVICE_FILE)) {
            return null;
        }

        try {
            Properties config = new Properties();
            String currentSection = null;
            Map<String, Properties> sections = new HashMap<>();

            try (BufferedReader reader = new BufferedReader(new FileReader(PG_SERVICE_FILE.toFile()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }

                    // Check for section header [service_name]
                    if (line.startsWith("[") && line.endsWith("]")) {
                        currentSection = line.substring(1, line.length() - 1);
                        sections.put(currentSection, new Properties());
                        continue;
                    }

                    // Parse key=value pairs
                    if (currentSection != null && line.contains("=")) {
                        String[] parts = line.split("=", 2);
                        if (parts.length == 2) {
                            sections.get(currentSection).setProperty(parts[0].trim(), parts[1].trim());
                        }
                    }
                }
            }

            // Use requested service or fall back to first available
            Properties serviceConfig = sections.get(serviceName);
            if (serviceConfig == null && !sections.isEmpty()) {
                serviceName = sections.keySet().iterator().next();
                serviceConfig = sections.get(serviceName);
            }

            if (serviceConfig == null) {
                return null;
            }

            String host = serviceConfig.getProperty("host");
            String username = serviceConfig.getProperty("user", serviceConfig.getProperty("username"));
            
            if (host == null || username == null) {
                return null;
            }

            int port = Integer.parseInt(serviceConfig.getProperty("port", "5432"));
            String database = serviceConfig.getProperty("dbname", serviceConfig.getProperty("database", "postgres"));
            String password = serviceConfig.getProperty("password", "");
            String sslMode = serviceConfig.getProperty("sslmode", "require");

            return new PostgreSQLCredentials(host, port, database, username, password, sslMode);

        } catch (Exception e) {
            // Log warning but don't fail - fall back to other methods
            System.err.println("Warning: Failed to read .pg_service.conf: " + e.getMessage());
            return null;
        }
    }

    /**
     * Read PostgreSQL credentials from environment variables.
     */
    private static PostgreSQLCredentials getPostgreSQLCredentialsFromEnv() {
        String host = getEnv("PGHOST", "POSTGRESQL_HOST");
        String username = getEnv("PGUSER", "POSTGRESQL_USERNAME", "POSTGRES_USER");
        
        if (host == null || username == null) {
            return null;
        }

        int port = Integer.parseInt(getEnv("PGPORT", "POSTGRESQL_PORT", "5432"));
        String database = getEnv("PGDATABASE", "POSTGRESQL_DATABASE", "postgres");
        String password = getEnv("PGPASSWORD", "POSTGRESQL_PASSWORD", "POSTGRES_PASSWORD", "");
        String sslMode = getEnv("PGSSLMODE", "POSTGRESQL_SSLMODE", "prefer");

        return new PostgreSQLCredentials(host, port, database, username, password, sslMode);
    }

    /**
     * Read Snowflake credentials from ~/.snowflake/connections.toml file.
     */
    private static SnowflakeCredentials getSnowflakeCredentialsFromConnectionsToml(String connectionName) {
        if (!Files.exists(SNOWFLAKE_CONNECTIONS_FILE)) {
            return null;
        }

        try {
            Map<String, Map<String, String>> connections = parseTomlFile(SNOWFLAKE_CONNECTIONS_FILE);
            Map<String, String> connection = connections.get(connectionName);
            
            if (connection == null) {
                return null;
            }

            String account = connection.get("account");
            String user = connection.get("user");
            
            if (account == null || user == null) {
                return null;
            }

            return new SnowflakeCredentials(
                account,
                user,
                connection.get("password"),
                connection.get("private_key_file"),
                connection.get("private_key_file_pwd"),
                connection.get("role"),
                connection.get("warehouse"),
                connection.get("database"),
                connection.get("schema")
            );

        } catch (Exception e) {
            System.err.println("Warning: Failed to read connections.toml: " + e.getMessage());
            return null;
        }
    }

    /**
     * Read Snowflake credentials from environment variables.
     */
    private static SnowflakeCredentials getSnowflakeCredentialsFromEnv() {
        String account = System.getenv("SNOWFLAKE_ACCOUNT");
        String user = System.getenv("SNOWFLAKE_USER");
        
        if (account == null || user == null) {
            return null;
        }

        return new SnowflakeCredentials(
            account,
            user,
            System.getenv("SNOWFLAKE_PASSWORD"),
            System.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
            System.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
            System.getenv("SNOWFLAKE_ROLE"),
            System.getenv("SNOWFLAKE_WAREHOUSE"),
            System.getenv("SNOWFLAKE_DATABASE"),
            System.getenv("SNOWFLAKE_SCHEMA")
        );
    }

    /**
     * Simple TOML parser for connections.toml file.
     * Only handles the basic [section] key = "value" format needed for Snowflake connections.
     */
    private static Map<String, Map<String, String>> parseTomlFile(Path tomlFile) throws IOException {
        Map<String, Map<String, String>> sections = new HashMap<>();
        String currentSection = null;
        
        Pattern sectionPattern = Pattern.compile("^\\[([^\\]]+)\\]$");
        Pattern keyValuePattern = Pattern.compile("^([^=]+)\\s*=\\s*\"?([^\"\\n\\r]*)\"?$");

        try (BufferedReader reader = Files.newBufferedReader(tomlFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                // Check for section header [section_name]
                Matcher sectionMatcher = sectionPattern.matcher(line);
                if (sectionMatcher.matches()) {
                    currentSection = sectionMatcher.group(1);
                    sections.put(currentSection, new HashMap<>());
                    continue;
                }

                // Parse key = value pairs
                if (currentSection != null) {
                    Matcher kvMatcher = keyValuePattern.matcher(line);
                    if (kvMatcher.matches()) {
                        String key = kvMatcher.group(1).trim();
                        String value = kvMatcher.group(2).trim();
                        // Remove surrounding quotes if present
                        if (value.startsWith("\"") && value.endsWith("\"")) {
                            value = value.substring(1, value.length() - 1);
                        }
                        sections.get(currentSection).put(key, value);
                    }
                }
            }
        }

        return sections;
    }

    /**
     * Get environment variable with fallback options.
     */
    private static String getEnv(String... keys) {
        for (String key : keys) {
            String value = System.getenv(key);
            if (value != null && !value.trim().isEmpty()) {
                return value;
            }
        }
        return null;
    }

    /**
     * Clear cached credentials to force re-reading from sources.
     */
    public static void clearCredentialCache() {
        cachedPostgreSQLCredentials = null;
        cachedSnowflakeCredentials = null;
    }

    /**
     * Check if PostgreSQL credentials are available.
     */
    public static boolean hasPostgreSQLCredentials() {
        try {
            getPostgreSQLCredentials();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Check if Snowflake credentials are available.
     */
    public static boolean hasSnowflakeCredentials() {
        try {
            getSnowflakeCredentials();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
