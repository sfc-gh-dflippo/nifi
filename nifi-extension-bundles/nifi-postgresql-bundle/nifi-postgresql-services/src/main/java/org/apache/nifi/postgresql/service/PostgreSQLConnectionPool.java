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
 
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.AbstractDBCPConnectionPool;
 
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
 
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionWrapper;
import org.apache.nifi.processors.postgresql.util.ConnectionSettings;
 
import org.apache.nifi.postgresql.service.util.ConnectionUrlFormat;
import org.apache.nifi.postgresql.service.util.ConnectionPoolSettings;

import java.sql.Connection;
//import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
            SOFT_MIN_EVICTABLE_IDLE_TIME
    );

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
}
