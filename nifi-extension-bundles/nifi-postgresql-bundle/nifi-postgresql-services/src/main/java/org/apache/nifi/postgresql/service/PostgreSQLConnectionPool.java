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

import org.postgresql.Driver;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
 
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.dbcp.AbstractDBCPConnectionPool;
 
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
 
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionWrapper;
import org.apache.nifi.processors.postgresql.util.ConnectionSettings;
import org.apache.nifi.processors.postgresql.util.TableMetadata;
import org.apache.nifi.postgresql.service.util.TableMetadataCache;
import org.apache.nifi.postgresql.service.util.TableMetadataImpl;
 
import org.apache.nifi.postgresql.service.util.ConnectionUrlFormat;
import org.apache.nifi.postgresql.service.util.ConnectionPoolSettings;

import java.sql.Connection;
//import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_USER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_TOTAL_CONNECTIONS;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_WAIT_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.VALIDATION_QUERY;
import static org.apache.nifi.dbcp.utils.DBCPProperties.extractMillisWithInfinite;

/**
 * Connection Pooling Service for PostgreSQL built on Apache DBCP.
 * Exposes PostgreSQL JDBC settings and integrates with NiFi Controller Services.
 */
@Tags({"postgresql", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides PostgreSQL Connection Pooling Service. Supports full JDBC URL or host/port/database configuration and exposes common PostgreSQL JDBC driver parameters including SSL/TLS, Kerberos/GSS, query mode, autosave, prepared statement caching, timeouts, socket buffer sizes, and default row fetch size. Properties support Expression Language where indicated.")
@SupportsSensitiveDynamicProperties
@DynamicProperties({
        @DynamicProperty(name = "JDBC property name",
                value = "PostgreSQL JDBC property value",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "PostgreSQL JDBC driver property name and value applied to JDBC connections.")
})
@RequiresInstanceClassLoading
public class PostgreSQLConnectionPool extends AbstractDBCPConnectionPool implements PostgreSQLConnectionProviderService {

    public static final PropertyDescriptor CONNECTION_URL_FORMAT = ConnectionPoolSettings.CONNECTION_URL_FORMAT;

    public static final PropertyDescriptor POSTGRESQL_URL = ConnectionPoolSettings.POSTGRESQL_URL;

    public static final PropertyDescriptor POSTGRESQL_HOST_NAME = ConnectionPoolSettings.POSTGRESQL_HOST_NAME;

    public static final PropertyDescriptor POSTGRESQL_PORT = ConnectionPoolSettings.POSTGRESQL_PORT;

    public static final PropertyDescriptor POSTGRESQL_DATABASE = ConnectionPoolSettings.POSTGRESQL_DATABASE;

    public static final PropertyDescriptor POSTGRESQL_SCHEMA = ConnectionPoolSettings.POSTGRESQL_SCHEMA;

    public static final PropertyDescriptor POSTGRESQL_SSL_MODE = ConnectionPoolSettings.POSTGRESQL_SSL_MODE;

    public static final PropertyDescriptor POSTGRESQL_USER = ConnectionPoolSettings.POSTGRESQL_USER;

    public static final PropertyDescriptor POSTGRESQL_PASSWORD = ConnectionPoolSettings.POSTGRESQL_PASSWORD;

    public static final PropertyDescriptor METADATA_CACHE_TTL = new PropertyDescriptor.Builder()
            .name("metadata-cache-ttl")
            .displayName("Metadata Cache TTL")
            .description("Time-to-live for cached table metadata in minutes. Set to 0 to disable expiration (cache indefinitely). "
                    + "Cached metadata includes table columns, types, and primary keys. Cache is shared across all instances of this service.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_URL_FORMAT,
            POSTGRESQL_URL,
            POSTGRESQL_HOST_NAME,
            POSTGRESQL_PORT,
            POSTGRESQL_DATABASE,
            POSTGRESQL_SCHEMA,
            POSTGRESQL_SSL_MODE,
            POSTGRESQL_USER,
            POSTGRESQL_PASSWORD,
            ConnectionSettings.SSL,
            ConnectionSettings.SSL_CERTIFICATE,
            ConnectionSettings.SSL_KEY,
            ConnectionSettings.SSL_ROOT_CERTIFICATE,
            ConnectionSettings.APPLICATION_NAME,
            ConnectionSettings.LOGIN_TIMEOUT,
            ConnectionSettings.CONNECT_TIMEOUT,
            ConnectionSettings.SOCKET_TIMEOUT,
            ConnectionSettings.TCP_KEEPALIVE,
            ConnectionSettings.REWRITE_BATCHED_INSERTS,
            ConnectionSettings.STRINGTYPE,
            ConnectionSettings.BINARY_TRANSFER,
            ConnectionSettings.PREPARED_STATEMENT_CACHE_QUERIES,
            ConnectionSettings.PREPARED_STATEMENT_CACHE_SIZE_MIB,
            ConnectionSettings.PREPARED_STATEMENT_CACHE_SQL_LIMIT,
            ConnectionSettings.TARGET_SERVER_TYPE,
            ConnectionSettings.TARGET_SERVER_VERSION,
            ConnectionSettings.PREFERRED_QUERY_MODE,
            ConnectionSettings.AUTOSAVE,
            ConnectionSettings.CLEANUP_SAVEPOINTS,
            ConnectionSettings.KERBEROS_SERVER_NAME,
            ConnectionSettings.JAAS_APPLICATION_NAME,
            ConnectionSettings.GSS_ENC_MODE,
            ConnectionSettings.SSL_PASSWORD,
            ConnectionSettings.SSL_HOSTNAME_VERIFIER,
            ConnectionSettings.SSL_FACTORY,
            ConnectionSettings.SSL_FACTORY_ARG,
            ConnectionSettings.CANCEL_SIGNAL_TIMEOUT,
            ConnectionSettings.RECEIVE_BUFFER_SIZE,
            ConnectionSettings.SEND_BUFFER_SIZE,
            ConnectionSettings.ESCAPE_SYNTAX_CALL_MODE,
            ConnectionSettings.PREPARE_THRESHOLD,
            ConnectionSettings.DEFAULT_ROW_FETCH_SIZE,
            ConnectionSettings.ADAPTIVE_FETCH,
            ConnectionSettings.MAX_RESULT_BUFFER,
            VALIDATION_QUERY,
            MAX_WAIT_TIME,
            MAX_TOTAL_CONNECTIONS,
            MIN_IDLE,
            MAX_IDLE,
            MAX_CONN_LIFETIME,
            EVICTION_RUN_PERIOD,
            MIN_EVICTABLE_IDLE_TIME,
            SOFT_MIN_EVICTABLE_IDLE_TIME,
            METADATA_CACHE_TTL
    );

    private volatile long metadataCacheTtlMs = 0; // 0 = no expiration

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (METADATA_CACHE_TTL.equals(descriptor)) {
            // Convert minutes to milliseconds
            final int ttlMinutes = newValue == null ? 0 : Integer.parseInt(newValue);
            metadataCacheTtlMs = ttlMinutes * 60 * 1000L;
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR);

        return builder.build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        return Collections.emptyList();
    }

    @Override
    protected DataSourceConfiguration getDataSourceConfiguration(final ConfigurationContext context) {
        final String url = getUrl(context);
        final String driverName = Driver.class.getName();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());

        return new DataSourceConfiguration.Builder(url, driverName, user, password)
                .maxTotal(maxTotal)
                .validationQuery(validationQuery)
                .maxWaitMillis(maxWaitMillis)
                .minIdle(minIdle)
                .maxIdle(maxIdle)
                .maxConnLifetimeMillis(maxConnLifetimeMillis)
                .timeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis)
                .minEvictableIdleTimeMillis(minEvictableIdleTimeMillis)
                .softMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis)
                .build();
    }

    protected String getUrl(final ConfigurationContext context) {
        final ConnectionUrlFormat connectionUrlFormat = context.getProperty(CONNECTION_URL_FORMAT).asAllowableValue(ConnectionUrlFormat.class);
        final ConnectionPoolSettings parameters = getConnectionUrlFormatParameters(context);

        return connectionUrlFormat.buildConnectionUrl(parameters);
    }

    @Override
    protected java.sql.Driver getDriver(final String driverName, final String url) {
        try {
            Class.forName(driverName);
            return DriverManager.getDriver(url);
        } catch (Exception e) {
            throw new ProcessException("PostgreSQL driver unavailable or incompatible connection URL", e);
        }
    }

    @Override
    protected Map<String, String> getConnectionProperties(final ConfigurationContext context) {
        final String database = context.getProperty(ConnectionSettings.DATABASE).evaluateAttributeExpressions().getValue();
        final String schema = context.getProperty(ConnectionSettings.SCHEMA).evaluateAttributeExpressions().getValue();
        final String ssl = context.getProperty(ConnectionSettings.SSL).evaluateAttributeExpressions().getValue();
        final String sslMode = context.getProperty(ConnectionSettings.SSL_MODE).evaluateAttributeExpressions().getValue();
        final String sslCertificate = context.getProperty(ConnectionSettings.SSL_CERTIFICATE).evaluateAttributeExpressions().getValue();
        final String sslKey = context.getProperty(ConnectionSettings.SSL_KEY).evaluateAttributeExpressions().getValue();
        final String sslRootCertificate = context.getProperty(ConnectionSettings.SSL_ROOT_CERTIFICATE).evaluateAttributeExpressions().getValue();
        final String applicationName = context.getProperty(ConnectionSettings.APPLICATION_NAME).evaluateAttributeExpressions().getValue();
        final String loginTimeout = context.getProperty(ConnectionSettings.LOGIN_TIMEOUT).evaluateAttributeExpressions().getValue();
        final String connectTimeout = context.getProperty(ConnectionSettings.CONNECT_TIMEOUT).evaluateAttributeExpressions().getValue();
        final String socketTimeout = context.getProperty(ConnectionSettings.SOCKET_TIMEOUT).evaluateAttributeExpressions().getValue();
        final String tcpKeepAlive = context.getProperty(ConnectionSettings.TCP_KEEPALIVE).evaluateAttributeExpressions().getValue();
        final String rewriteBatchedInserts = context.getProperty(ConnectionSettings.REWRITE_BATCHED_INSERTS).evaluateAttributeExpressions().getValue();
        final String stringtype = context.getProperty(ConnectionSettings.STRINGTYPE).evaluateAttributeExpressions().getValue();
        final String binaryTransfer = context.getProperty(ConnectionSettings.BINARY_TRANSFER).evaluateAttributeExpressions().getValue();
        final String psCacheQueries = context.getProperty(ConnectionSettings.PREPARED_STATEMENT_CACHE_QUERIES).evaluateAttributeExpressions().getValue();
        final String psCacheSizeMiB = context.getProperty(ConnectionSettings.PREPARED_STATEMENT_CACHE_SIZE_MIB).evaluateAttributeExpressions().getValue();
        final String psCacheSqlLimit = context.getProperty(ConnectionSettings.PREPARED_STATEMENT_CACHE_SQL_LIMIT).evaluateAttributeExpressions().getValue();
        final String targetServerType = context.getProperty(ConnectionSettings.TARGET_SERVER_TYPE).evaluateAttributeExpressions().getValue();
        final String targetServerVersion = context.getProperty(ConnectionSettings.TARGET_SERVER_VERSION).evaluateAttributeExpressions().getValue();
        final String preferredQueryMode = context.getProperty(ConnectionSettings.PREFERRED_QUERY_MODE).evaluateAttributeExpressions().getValue();
        final String autosave = context.getProperty(ConnectionSettings.AUTOSAVE).evaluateAttributeExpressions().getValue();
        final String cleanupSavepoints = context.getProperty(ConnectionSettings.CLEANUP_SAVEPOINTS).evaluateAttributeExpressions().getValue();
        final String kerberosServerName = context.getProperty(ConnectionSettings.KERBEROS_SERVER_NAME).evaluateAttributeExpressions().getValue();
        final String jaasApplicationName = context.getProperty(ConnectionSettings.JAAS_APPLICATION_NAME).evaluateAttributeExpressions().getValue();
        final String gssEncMode = context.getProperty(ConnectionSettings.GSS_ENC_MODE).evaluateAttributeExpressions().getValue();
        final String sslPassword = context.getProperty(ConnectionSettings.SSL_PASSWORD).evaluateAttributeExpressions().getValue();
        final String sslHostnameVerifier = context.getProperty(ConnectionSettings.SSL_HOSTNAME_VERIFIER).evaluateAttributeExpressions().getValue();
        final String sslFactory = context.getProperty(ConnectionSettings.SSL_FACTORY).evaluateAttributeExpressions().getValue();
        final String sslFactoryArg = context.getProperty(ConnectionSettings.SSL_FACTORY_ARG).evaluateAttributeExpressions().getValue();
        final String cancelSignalTimeout = context.getProperty(ConnectionSettings.CANCEL_SIGNAL_TIMEOUT).evaluateAttributeExpressions().getValue();
        final String receiveBufferSize = context.getProperty(ConnectionSettings.RECEIVE_BUFFER_SIZE).evaluateAttributeExpressions().getValue();
        final String sendBufferSize = context.getProperty(ConnectionSettings.SEND_BUFFER_SIZE).evaluateAttributeExpressions().getValue();
        final String defaultRowFetchSize = context.getProperty(ConnectionSettings.DEFAULT_ROW_FETCH_SIZE).evaluateAttributeExpressions().getValue();

        final Map<String, String> connectionProperties = super.getConnectionProperties(context);
        if (database != null) connectionProperties.put("database", database);
        if (schema != null) connectionProperties.put("currentSchema", schema);
        if (ssl != null && ssl.equalsIgnoreCase("true")) {
            connectionProperties.put("ssl", ssl);
            if (sslMode != null) connectionProperties.put("sslmode", sslMode);
            if (sslCertificate != null) connectionProperties.put("sslcert", sslCertificate);
            if (sslKey != null) connectionProperties.put("sslkey", sslKey);
            if (sslRootCertificate != null) connectionProperties.put("sslrootcert", sslRootCertificate);
        }
        if (applicationName != null) connectionProperties.put("ApplicationName", applicationName);
        if (loginTimeout != null) connectionProperties.put("loginTimeout", loginTimeout);
        if (connectTimeout != null) connectionProperties.put("connectTimeout", connectTimeout);
        if (socketTimeout != null) connectionProperties.put("socketTimeout", socketTimeout);
        if (tcpKeepAlive != null) connectionProperties.put("tcpKeepAlive", tcpKeepAlive);
        if (rewriteBatchedInserts != null) connectionProperties.put("reWriteBatchedInserts", rewriteBatchedInserts);
        if (stringtype != null) connectionProperties.put("stringtype", stringtype);
        if (binaryTransfer != null) connectionProperties.put("binaryTransfer", binaryTransfer);
        if (preferredQueryMode != null) connectionProperties.put("preferQueryMode", preferredQueryMode);
        if (psCacheQueries != null) connectionProperties.put("preparedStatementCacheQueries", psCacheQueries);
        if (psCacheSizeMiB != null) connectionProperties.put("preparedStatementCacheSizeMiB", psCacheSizeMiB);
        if (psCacheSqlLimit != null) connectionProperties.put("preparedStatementCacheSqlLimit", psCacheSqlLimit);
        if (targetServerType != null) connectionProperties.put("targetServerType", targetServerType);
        if (targetServerVersion != null) connectionProperties.put("assumeMinServerVersion", targetServerVersion);
        if (autosave != null) connectionProperties.put("autosave", autosave);
        if (cleanupSavepoints != null) connectionProperties.put("cleanupSavepoints", cleanupSavepoints);
        if (kerberosServerName != null) connectionProperties.put("kerberosServerName", kerberosServerName);
        if (jaasApplicationName != null) connectionProperties.put("jaasApplicationName", jaasApplicationName);
        if (gssEncMode != null) connectionProperties.put("gssEncMode", gssEncMode);
        if (sslPassword != null) connectionProperties.put("sslpassword", sslPassword);
        if (sslHostnameVerifier != null) connectionProperties.put("sslhostnameverifier", sslHostnameVerifier);
        if (sslFactory != null) connectionProperties.put("sslfactory", sslFactory);
        if (sslFactoryArg != null) connectionProperties.put("sslfactoryarg", sslFactoryArg);
        if (cancelSignalTimeout != null) connectionProperties.put("cancelSignalTimeout", cancelSignalTimeout);
        if (receiveBufferSize != null) connectionProperties.put("receiveBufferSize", receiveBufferSize);
        if (sendBufferSize != null) connectionProperties.put("sendBufferSize", sendBufferSize);
        if (defaultRowFetchSize != null) connectionProperties.put("defaultRowFetchSize", defaultRowFetchSize);
        final String escapeSyntaxCallMode = context.getProperty(ConnectionSettings.ESCAPE_SYNTAX_CALL_MODE).evaluateAttributeExpressions().getValue();
        final String prepareThreshold = context.getProperty(ConnectionSettings.PREPARE_THRESHOLD).evaluateAttributeExpressions().getValue();
        final String adaptiveFetch = context.getProperty(ConnectionSettings.ADAPTIVE_FETCH).evaluateAttributeExpressions().getValue();
        final String maxResultBuffer = context.getProperty(ConnectionSettings.MAX_RESULT_BUFFER).evaluateAttributeExpressions().getValue();
        if (escapeSyntaxCallMode != null) connectionProperties.put("escapeSyntaxCallMode", escapeSyntaxCallMode);
        if (prepareThreshold != null) connectionProperties.put("prepareThreshold", prepareThreshold);
        if (adaptiveFetch != null) connectionProperties.put("adaptiveFetch", adaptiveFetch);
        if (maxResultBuffer != null) connectionProperties.put("maxResultBuffer", maxResultBuffer);

        return connectionProperties;
    }

    @Override
    public PostgreSQLConnectionWrapper getPostgreSQLConnection() {
        final Connection connection = getConnection();
        try {
            connection.setAutoCommit(false);
        } catch (Exception e) {
            throw new ProcessException("Failed to set auto-commit to false on PostgreSQL connection", e);
        }
        return new PostgreSQLConnectionWrapper(connection);
    }

    private ConnectionPoolSettings getConnectionUrlFormatParameters(ConfigurationContext context) {
        return new ConnectionPoolSettings(
                context.getProperty(POSTGRESQL_URL).evaluateAttributeExpressions().getValue(),
                context.getProperty(POSTGRESQL_HOST_NAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(POSTGRESQL_PORT).evaluateAttributeExpressions().getValue(),
                context.getProperty(POSTGRESQL_DATABASE).evaluateAttributeExpressions().getValue(),
                context.getProperty(POSTGRESQL_SCHEMA).evaluateAttributeExpressions().getValue(),
                context.getProperty(ConnectionSettings.SSL).evaluateAttributeExpressions().getValue(),
                context.getProperty(POSTGRESQL_SSL_MODE).evaluateAttributeExpressions().getValue(),
                context.getProperty(ConnectionSettings.SSL_CERTIFICATE).evaluateAttributeExpressions().getValue(),
                context.getProperty(ConnectionSettings.SSL_KEY).evaluateAttributeExpressions().getValue(),
                context.getProperty(ConnectionSettings.SSL_ROOT_CERTIFICATE).evaluateAttributeExpressions().getValue()
        );
    }

    /**
     * Override verify to provide enhanced error messages with root cause extraction for PostgreSQL connections.
     * When connection fails, this method attempts a direct connection using the PostgreSQL JDBC driver
     * to retrieve the actual error message from PostgreSQL.
     */
    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            // Attempt the standard verification from parent class
            final List<ConfigVerificationResult> parentResults = super.verify(context, verificationLogger, variables);
            
            // Enhance any failure results with better PostgreSQL-specific diagnostics
            for (ConfigVerificationResult result : parentResults) {
                if (result.getOutcome() == FAILED && result.getVerificationStepName().equals("Establish Connection")) {
                    // Get the actual PostgreSQL error by attempting a direct connection
                    final String enhancedExplanation = getDirectConnectionError(context, verificationLogger);
                    
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName(result.getVerificationStepName())
                            .outcome(FAILED)
                            .explanation(enhancedExplanation)
                            .build());
                } else {
                    // Keep other results as-is
                    results.add(result);
                }
            }
            
            return results;
            
        } catch (final Exception e) {
            verificationLogger.error("Unexpected error during verification", e);
            final String rootCauseMessage = getRootCauseMessage(e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Verify Configuration")
                    .outcome(FAILED)
                    .explanation("Verification failed: " + rootCauseMessage)
                    .build());
            return results;
        }
    }

    /**
     * Attempt a direct connection using PostgreSQL JDBC driver to get the actual error message.
     * This bypasses DBCP connection pooling to retrieve the raw PostgreSQL error.
     */
    private String getDirectConnectionError(final ConfigurationContext context, final ComponentLog logger) {
        final StringBuilder enhanced = new StringBuilder();
        enhanced.append("Failed to establish PostgreSQL connection.\n\n");
        
        // Get connection parameters
        final String jdbcUrl = getUrl(context);
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        
        // Display connection details (with masked password)
        final String maskedUrl = jdbcUrl.replaceAll("([&?]password=)[^&]*", "$1***");
        enhanced.append("Connection URL: ").append(maskedUrl).append("\n");
        
        try {
            final String host = context.getProperty(POSTGRESQL_HOST_NAME).evaluateAttributeExpressions().getValue();
            final String port = context.getProperty(POSTGRESQL_PORT).evaluateAttributeExpressions().getValue();
            final String database = context.getProperty(POSTGRESQL_DATABASE).evaluateAttributeExpressions().getValue();
            
            if (host != null) enhanced.append("Host: ").append(host).append("\n");
            if (port != null) enhanced.append("Port: ").append(port).append("\n");
            if (database != null) enhanced.append("Database: ").append(database).append("\n");
        } catch (Exception e) {
            // Continue even if we can't extract connection details
        }
        
        // Attempt direct connection to get actual PostgreSQL error
        enhanced.append("\nActual PostgreSQL Error:\n");
        Connection testConnection = null;
        try {
            // Load PostgreSQL driver
            Class.forName(Driver.class.getName());
            
            // Get connection properties
            final Map<String, String> connectionProperties = getConnectionProperties(context);
            final Properties props = new Properties();
            if (user != null) props.setProperty("user", user);
            if (password != null) props.setProperty("password", password);
            
            // Add all other connection properties
            for (Map.Entry<String, String> entry : connectionProperties.entrySet()) {
                if (entry.getValue() != null && !entry.getKey().equals("user") && !entry.getKey().equals("password")) {
                    props.setProperty(entry.getKey(), entry.getValue());
                }
            }
            
            // Attempt direct connection - this will throw the actual PostgreSQL exception
            testConnection = DriverManager.getConnection(jdbcUrl, props);
            
            // If we got here, connection actually succeeded (shouldn't happen in this path)
            enhanced.append("Connection succeeded unexpectedly during direct test.\n");
            
        } catch (final Exception e) {
            // This is what we want - the actual PostgreSQL error
            final String actualError = extractPostgreSQLError(e);
            enhanced.append(actualError).append("\n");
            
        } finally {
            if (testConnection != null) {
                try {
                    testConnection.close();
                } catch (Exception e) {
                    logger.debug("Error closing test connection", e);
                }
            }
        }
        
        // Add troubleshooting guidance
        enhanced.append("\nTroubleshooting Tips:\n");
        enhanced.append("1. Verify PostgreSQL is running and accessible at the specified host and port\n");
        enhanced.append("2. Check that the database exists and the user has access permissions\n");
        enhanced.append("3. If you are using Snowflake, verify your Snowflake network rules and external access integration is configured to allow connections to PostgreSQL\n");
        enhanced.append("4. Verify correct port (PostgreSQL default is 5432)\n");
        enhanced.append("5. Test connectivity outside of NiFi using the psql command: psql -h <host> -p <port> -U <user> -d <database>\n");
        enhanced.append("6. For SSL connections, verify SSL mode and certificate configurations\n");
        
        return enhanced.toString();
    }

    /**
     * Extract the most useful error message from a PostgreSQL connection exception.
     * Traverses the exception chain to find the most specific PostgreSQL error,
     * including IOException details, SQL state, and suppressed exceptions.
     */
    private String extractPostgreSQLError(final Throwable throwable) {
        if (throwable == null) {
            return "Unknown connection error";
        }
        
        final StringBuilder errorDetails = new StringBuilder();
        
        // Traverse the exception chain to find the most informative message
        Throwable current = throwable;
        String sqlState = null;
        Integer errorCode = null;
        String detailedMessage = null;
        String ioExceptionMessage = null;
        
        while (current != null) {
            final String message = current.getMessage();
            final String exceptionType = current.getClass().getSimpleName();
            final String fullClassName = current.getClass().getName();
            
            // Extract SQL state and error code from SQLException
            if (current instanceof SQLException) {
                final SQLException sqlEx = (SQLException) current;
                if (sqlState == null) {
                    sqlState = sqlEx.getSQLState();
                }
                if (errorCode == null) {
                    errorCode = sqlEx.getErrorCode();
                }
            }
            
            // IO exceptions usually have the real network error details
            if (fullClassName.contains("IOException") || fullClassName.contains("SocketException") || 
                fullClassName.contains("UnknownHostException") || fullClassName.contains("ConnectException")) {
                if (message != null && !message.trim().isEmpty() && !message.equals("Connection refused")) {
                    ioExceptionMessage = exceptionType + ": " + message;
                } else if (message != null) {
                    ioExceptionMessage = exceptionType;
                }
            }
            
            // Look for specific error patterns that are more informative than generic messages
            if (message != null && !message.contains("The connection attempt failed")) {
                if (message.contains("Connection refused") ||
                    message.contains("authentication failed") ||
                    message.contains("password authentication failed") ||
                    message.contains("no pg_hba.conf entry") ||
                    message.contains("database") && message.contains("does not exist") ||
                    message.contains("timeout") ||
                    message.contains("timed out") ||
                    message.contains("Unknown host") ||
                    message.contains("UnknownHostException") ||
                    message.contains("No route to host") ||
                    message.contains("SSL") ||
                    message.contains("FATAL") ||
                    message.contains("Connection reset") ||
                    message.contains("Connection closed")) {
                    detailedMessage = message;
                    break; // Found a specific error, use it
                }
            }
            
            // Check suppressed exceptions
            if (current.getSuppressed() != null && current.getSuppressed().length > 0) {
                for (Throwable suppressed : current.getSuppressed()) {
                    String suppressedMsg = suppressed.getMessage();
                    if (suppressedMsg != null && !suppressedMsg.contains("The connection attempt failed")) {
                        detailedMessage = suppressedMsg;
                        break;
                    }
                }
            }
            
            current = current.getCause();
        }
        
        // Build the error message with all available details
        if (detailedMessage != null) {
            errorDetails.append(detailedMessage);
        } else if (ioExceptionMessage != null) {
            errorDetails.append(ioExceptionMessage);
        } else {
            // Last resort - use the original message
            errorDetails.append(throwable.getMessage() != null ? throwable.getMessage() : throwable.getClass().getSimpleName());
        }
        
        // Add SQL state if available
        if (sqlState != null) {
            errorDetails.append(" (SQL State: ").append(sqlState).append(")");
        }
        
        // Add error code if available and meaningful
        if (errorCode != null && errorCode != 0) {
            errorDetails.append(" (Error Code: ").append(errorCode).append(")");
        }
        
        // Add exception type context if the message is very generic
        String result = errorDetails.toString();
        if (result.equals("The connection attempt failed") || result.trim().isEmpty()) {
            // Try to get the deepest exception type for context
            Throwable deepest = throwable;
            while (deepest.getCause() != null) {
                deepest = deepest.getCause();
            }
            result = "Connection failed - " + deepest.getClass().getSimpleName() + 
                     (deepest.getMessage() != null ? ": " + deepest.getMessage() : "");
        }
        
        return result;
    }


    /**
     * Extract the root cause message from an exception, traversing the entire cause chain.
     * This is critical for PostgreSQL connections because DBCP wraps exceptions multiple times.
     * 
     * @param throwable The exception to analyze
     * @return The root cause message with exception type
     */
    private String getRootCauseMessage(final Throwable throwable) {
        if (throwable == null) {
            return "Unknown error";
        }
        
        // Build a chain of all exception messages to provide complete context
        final List<String> errorChain = new ArrayList<>();
        Throwable current = throwable;
        Throwable rootCause = throwable;
        
        // Traverse the exception chain
        while (current != null) {
            rootCause = current;  // Keep updating to get the deepest cause
            
            final String message = current.getMessage();
            final String exceptionType = current.getClass().getSimpleName();
            
            if (message != null && !message.trim().isEmpty()) {
                // Add exception type and message if not already in chain
                final String fullMessage = exceptionType + ": " + message;
                if (!errorChain.contains(fullMessage)) {
                    errorChain.add(fullMessage);
                }
            }
            
            current = current.getCause();
        }
        
        // Build the comprehensive error message
        final StringBuilder result = new StringBuilder();
        
        if (!errorChain.isEmpty()) {
            // Show the root cause first (most specific error)
            result.append(errorChain.get(errorChain.size() - 1));
            
            // If there are multiple layers, show the chain
            if (errorChain.size() > 1) {
                result.append("\n\nException Chain (outer to inner):");
                for (int i = 0; i < errorChain.size(); i++) {
                    result.append("\n  ").append(i + 1).append(". ").append(errorChain.get(i));
                }
            }
        } else {
            // Fallback if no messages found
            result.append(rootCause.getClass().getName());
        }
        
        return result.toString();
    }

    // ========== Metadata Cache Implementation ==========

    public TableMetadata getTableMetadata(final String schema, final String table) {
        return getTableMetadata(schema, table, false);
    }

    public TableMetadata getTableMetadata(final String schema, final String table, final boolean forceRefresh) {
        final TableMetadataCache cache = TableMetadataCache.getInstance();

        // Try to get from cache first if not forcing refresh
        if (!forceRefresh) {
            final TableMetadata cached = cache.get(getCurrentDatabase(), schema, table, metadataCacheTtlMs);
            if (cached != null) {
                return cached;
            }
        }

        // Fetch fresh metadata
        try (final PostgreSQLConnectionWrapper wrapper = getPostgreSQLConnection()) {
            final Connection conn = wrapper.getConnection();
            final String dbName = getCurrentDatabase();
            final TableMetadata metadata = TableMetadataImpl.fetch(conn, schema, table);
            cache.put(dbName, schema, table, metadata);
            return metadata;
        } catch (Exception e) {
            throw new ProcessException("Failed to fetch table metadata for " + schema + "." + table, e);
        }
    }

    /**
     * Retrieves cached table metadata, fetching from the database if not already cached or if forceRefresh is true.
     *
     * @param schema the schema name
     * @param table  the table name
     * @return the table metadata
     */
    public TableMetadata getTableMetadataWithColumnValidation(final String schema, final String table, final List<String> incomingColumns) {
        // Get cached metadata (or fetch if not cached)
        TableMetadata metadata = getTableMetadata(schema, table, false);

        // Check if all incoming columns exist in the cached metadata
        if (!metadata.hasAllColumns(incomingColumns)) {
            // Column mismatch detected - fetch fresh metadata
            getLogger().debug("Column mismatch detected for {}.{}, refreshing metadata cache", schema, table);
            metadata = getTableMetadata(schema, table, true);
        }

        return metadata;
    }

    /**
     * Invalidates cached metadata for a specific table.
     *
     * @param schema the schema name
     * @param table  the table name
     */
    public void invalidateTableMetadata(final String schema, final String table) {
        final TableMetadataCache cache = TableMetadataCache.getInstance();
        cache.invalidate(getCurrentDatabase(), schema, table);
    }

    /**
     * Clears all cached table metadata.
     */
    public void clearMetadataCache() {
        final TableMetadataCache cache = TableMetadataCache.getInstance();
        cache.clear();
    }

    private String getCurrentDatabase() {
        // Try to get database from configuration
        try (final PostgreSQLConnectionWrapper wrapper = getPostgreSQLConnection()) {
            final Connection connection = wrapper.getConnection();
            return connection.getCatalog();
        } catch (Exception e) {
            getLogger().warn("Failed to get current database name, using 'unknown'", e);
            return "unknown";
        }
    }
}
