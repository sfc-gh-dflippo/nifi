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

package org.apache.nifi.processors.postgresql.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public final class ConnectionSettings {
    private ConnectionSettings() {
    }

    public static final PropertyDescriptor HOST_NAME = new PropertyDescriptor.Builder()
            .name("host-name")
            .displayName("Host Name")
            .description("PostgreSQL host name to use for connection.")
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("port")
            .displayName("Port")
            .description("PostgreSQL port to use for connection.")
            .defaultValue("5432")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();
 
    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("database")
            .displayName("Database")
            .description("The database to use by default. The same as passing 'db=DATABASE_NAME' to the connection string.")
            .defaultValue("postgres")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

        public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("schema")
            .displayName("Schema")
            .description("The schema to use by default. The same as passing 'currentSchema=SCHEMA_NAME' to the connection string.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor SSL = new PropertyDescriptor.Builder()
            .name("ssl")
            .displayName("SSL Enabled")
            .description("Whether to enable SSL for the connection. When enabled, additional SSL properties may be used.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

        // SSL Mode allowable values aligned with pgjdbc (sslmode)
        public static final AllowableValue SSL_MODE_DISABLE = new AllowableValue(
            "disable",
            "Disable",
            "Do not use SSL."
        );

        public static final AllowableValue SSL_MODE_ALLOW = new AllowableValue(
            "allow",
            "Allow",
            "Try non-SSL first, then fall back to SSL if required."
        );

        public static final AllowableValue SSL_MODE_PREFER = new AllowableValue(
            "prefer",
            "Prefer",
            "Try SSL first, then fall back to non-SSL if server does not support SSL."
        );

        public static final AllowableValue SSL_MODE_REQUIRE = new AllowableValue(
            "require",
            "Require",
            "Always require SSL, but do not validate certificates."
        );

        public static final AllowableValue SSL_MODE_VERIFY_CA = new AllowableValue(
            "verify-ca",
            "Verify CA",
            "Require SSL and verify that the server certificate is issued by a trusted CA."
        );

        public static final AllowableValue SSL_MODE_VERIFY_FULL = new AllowableValue(
            "verify-full",
            "Verify Full",
            "Require SSL and verify that the server certificate is issued by a trusted CA and that the hostname matches."
        );

        public static final PropertyDescriptor SSL_MODE = new PropertyDescriptor.Builder()
            .name("ssl-mode")
            .displayName("SSL Mode")
            .description("The SSL mode to use for the connection.")
            .defaultValue("prefer")
            .allowableValues(
                SSL_MODE_DISABLE,
                SSL_MODE_ALLOW,
                SSL_MODE_PREFER,
                SSL_MODE_REQUIRE,
                SSL_MODE_VERIFY_CA,
                SSL_MODE_VERIFY_FULL
            )
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

        public static final PropertyDescriptor SSL_CERTIFICATE = new PropertyDescriptor.Builder()
            .name("ssl-certificate")
            .displayName("SSL Certificate")
            .description("Path to client SSL certificate file. Defaults to ${user.home}/.postgresql/postgresql.crt. PEM encoded X509v3 certificate format.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor SSL_KEY = new PropertyDescriptor.Builder()
            .name("ssl-key")
            .displayName("SSL Key")
            .description("Path to client SSL key file. Defaults to ${user.home}/.postgresql/postgresql.pk8. PKCS-12 or PKCS-8 DER format.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor SSL_ROOT_CERTIFICATE = new PropertyDescriptor.Builder()
            .name("ssl-root-certificate")
            .displayName("SSL Root Certificate")
            .description("Path to the SSL root certificate. Defaults to ${user.home}/.postgresql/root.crt. PEM encoded X509v3 certificate format.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .name("application-name")
            .displayName("Application Name")
            .description("Sets applicationName PostgreSQL connection parameter.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor LOGIN_TIMEOUT = new PropertyDescriptor.Builder()
            .name("login-timeout")
            .displayName("Login Timeout (seconds)")
            .description("Maximum time in seconds to wait for a successful authentication.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connect-timeout")
            .displayName("Connect Timeout (seconds)")
            .description("Maximum time in seconds to establish a connection to the server.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor SOCKET_TIMEOUT = new PropertyDescriptor.Builder()
            .name("socket-timeout")
            .displayName("Socket Timeout (seconds)")
            .description("Read timeout in seconds for socket operations.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor TCP_KEEPALIVE = new PropertyDescriptor.Builder()
            .name("tcp-keep-alive")
            .displayName("TCP Keep Alive")
            .description("Enable TCP keep-alive probes.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor REWRITE_BATCHED_INSERTS = new PropertyDescriptor.Builder()
            .name("rewrite-batched-inserts")
            .displayName("Rewrite Batched Inserts")
            .description("Enable server-side rewriting of batched INSERT statements.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final AllowableValue STRINGTYPE_UNSPECIFIED = new AllowableValue(
            "unspecified",
            "Unspecified",
            "Send string parameters with unspecified type; server determines type."
        );

        public static final AllowableValue STRINGTYPE_VARCHAR = new AllowableValue(
            "varchar",
            "Varchar",
            "Send string parameters explicitly as VARCHAR."
        );

        public static final PropertyDescriptor STRINGTYPE = new PropertyDescriptor.Builder()
            .name("stringtype")
            .displayName("String Type Handling")
            .description("Controls how string parameters are sent: 'unspecified' or 'varchar'.")
            .allowableValues(STRINGTYPE_UNSPECIFIED, STRINGTYPE_VARCHAR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor BINARY_TRANSFER = new PropertyDescriptor.Builder()
            .name("binary-transfer")
            .displayName("Binary Transfer")
            .description("Prefer binary transfer for certain data types where supported.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor PREPARED_STATEMENT_CACHE_QUERIES = new PropertyDescriptor.Builder()
            .name("prepared-statement-cache-queries")
            .displayName("Prepared Statement Cache Queries")
            .description("Number of queries in the prepared statement cache.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor PREPARED_STATEMENT_CACHE_SIZE_MIB = new PropertyDescriptor.Builder()
            .name("prepared-statement-cache-size-mib")
            .displayName("Prepared Statement Cache Size (MiB)")
            .description("Memory size for the prepared statement cache in MiB.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor PREPARED_STATEMENT_CACHE_SQL_LIMIT = new PropertyDescriptor.Builder()
            .name("prepared-statement-cache-sql-limit")
            .displayName("Prepared Statement Cache SQL Limit")
            .description("Maximum length of SQL statements stored in the cache.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final AllowableValue TARGET_SERVER_TYPE_ANY = new AllowableValue(
            "any",
            "Any",
            "Connect to any server in the cluster."
        );

        public static final AllowableValue TARGET_SERVER_TYPE_PRIMARY = new AllowableValue(
            "primary",
            "Primary",
            "Connect only to the primary (read-write) server."
        );

        public static final AllowableValue TARGET_SERVER_TYPE_SECONDARY = new AllowableValue(
            "secondary",
            "Secondary",
            "Connect only to a secondary (read-only) server."
        );

        public static final AllowableValue TARGET_SERVER_TYPE_PREFER_PRIMARY = new AllowableValue(
            "preferPrimary",
            "Prefer Primary",
            "Prefer primary server, fall back to secondary if unavailable."
        );

        public static final AllowableValue TARGET_SERVER_TYPE_PREFER_SECONDARY = new AllowableValue(
            "preferSecondary",
            "Prefer Secondary",
            "Prefer secondary server, fall back to primary if unavailable."
        );

        public static final PropertyDescriptor TARGET_SERVER_TYPE = new PropertyDescriptor.Builder()
            .name("target-server-type")
            .displayName("Target Server Type")
            .description("Target server type for connections: any, primary, secondary, preferPrimary, preferSecondary.")
            .allowableValues(
                TARGET_SERVER_TYPE_ANY,
                TARGET_SERVER_TYPE_PRIMARY,
                TARGET_SERVER_TYPE_SECONDARY,
                TARGET_SERVER_TYPE_PREFER_PRIMARY,
                TARGET_SERVER_TYPE_PREFER_SECONDARY
            )
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor TARGET_SERVER_VERSION = new PropertyDescriptor.Builder()
            .name("target-server-version")
            .displayName("Target Server Version")
            .description("Assume this PostgreSQL server version for feature negotiation (e.g., 14, 15).")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final AllowableValue PREFERRED_QUERY_MODE_SIMPLE = new AllowableValue(
            "simple",
            "Simple",
            "Use simple query protocol for all queries."
        );

        public static final AllowableValue PREFERRED_QUERY_MODE_EXTENDED = new AllowableValue(
            "extended",
            "Extended",
            "Use extended query protocol for all queries."
        );

        public static final AllowableValue PREFERRED_QUERY_MODE_EXTENDED_FOR_PREPARED = new AllowableValue(
            "extendedForPrepared",
            "Extended For Prepared",
            "Use extended query protocol for prepared statements; simple protocol otherwise."
        );

        public static final PropertyDescriptor PREFERRED_QUERY_MODE = new PropertyDescriptor.Builder()
            .name("preferred-query-mode")
            .displayName("Preferred Query Mode")
            .description("PostgreSQL driver preferQueryMode: simple, extended, or extendedForPrepared.")
            .allowableValues(
                PREFERRED_QUERY_MODE_SIMPLE,
                PREFERRED_QUERY_MODE_EXTENDED,
                PREFERRED_QUERY_MODE_EXTENDED_FOR_PREPARED
            )
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final AllowableValue AUTOSAVE_NEVER = new AllowableValue(
            "never",
            "Never",
            "Do not automatically create savepoints."
        );

        public static final AllowableValue AUTOSAVE_CONSERVATIVE = new AllowableValue(
            "conservative",
            "Conservative",
            "Create savepoints in cases where a failure may invalidate the transaction."
        );

        public static final AllowableValue AUTOSAVE_ALWAYS = new AllowableValue(
            "always",
            "Always",
            "Always create savepoints around statements."
        );

        public static final PropertyDescriptor AUTOSAVE = new PropertyDescriptor.Builder()
            .name("autosave")
            .displayName("Autosave")
            .description("Controls autosave behavior: never, conservative, always.")
            .allowableValues(AUTOSAVE_NEVER, AUTOSAVE_CONSERVATIVE, AUTOSAVE_ALWAYS)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor CLEANUP_SAVEPOINTS = new PropertyDescriptor.Builder()
            .name("cleanup-savepoints")
            .displayName("Cleanup Savepoints")
            .description("Cleanup savepoints automatically after failures.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor KERBEROS_SERVER_NAME = new PropertyDescriptor.Builder()
            .name("kerberos-server-name")
            .displayName("Kerberos Server Name")
            .description("Kerberos server principal name (gsslib).")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor JAAS_APPLICATION_NAME = new PropertyDescriptor.Builder()
            .name("jaas-application-name")
            .displayName("JAAS Application Name")
            .description("JAAS application name for Kerberos authentication.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor GSS_ENC_MODE = new PropertyDescriptor.Builder()
            .name("gss-enc-mode")
            .displayName("GSS Encryption Mode")
            .description("GSS encryption mode: disable, allow, prefer, require.")
            .allowableValues("disable", "allow", "prefer", "require")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor SSL_PASSWORD = new PropertyDescriptor.Builder()
            .name("ssl-password")
            .displayName("SSL Password")
            .description("Password for encrypted SSL key (sslpassword).")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(true)
            .build();

        public static final PropertyDescriptor SSL_HOSTNAME_VERIFIER = new PropertyDescriptor.Builder()
            .name("ssl-hostname-verifier")
            .displayName("SSL Hostname Verifier")
            .description("Class name implementing javax.net.ssl.HostnameVerifier.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        // SSL Factory allowable values must be declared before the descriptor to avoid illegal forward reference
        public static final AllowableValue SSL_FACTORY_NON_VALIDATING = new AllowableValue(
            "org.postgresql.ssl.NonValidatingFactory",
            "Non-Validating Factory",
            "Disables certificate validation. Default behavior to minimize issues when certificates are not available."
        );

        public static final AllowableValue SSL_FACTORY_DEFAULT_JAVA = new AllowableValue(
            "org.postgresql.ssl.DefaultJavaSSLFactory",
            "Default Java SSL Factory",
            "Uses the default Java SSL context and certificate verification."
        );

        public static final AllowableValue SSL_FACTORY_LIBPQ = new AllowableValue(
            "org.postgresql.ssl.LibPQFactory",
            "LibPQ SSL Factory",
            "Uses libpq-compatible SSL settings (sslcert, sslkey, sslrootcert) for certificate verification."
        );

        public static final AllowableValue SSL_FACTORY_SINGLE_CERT_VALIDATING = new AllowableValue(
            "org.postgresql.ssl.SingleCertValidatingFactory",
            "Single Cert Validating Factory",
            "Validates server certificate against a single provided certificate."
        );

        public static final AllowableValue SSL_FACTORY_LIBPQ_JDBC4 = new AllowableValue(
            "org.postgresql.ssl.jdbc4.LibPQFactory",
            "LibPQ (JDBC4) SSL Factory",
            "Legacy JDBC4 variant of the LibPQ SSL factory."
        );

        public static final PropertyDescriptor SSL_FACTORY = new PropertyDescriptor.Builder()
            .name("ssl-factory")
            .displayName("SSL Factory")
            .description("The SSL factory implementation from the PostgreSQL JDBC driver used to create SSL sockets.")
            .allowableValues(
                SSL_FACTORY_NON_VALIDATING,
                SSL_FACTORY_DEFAULT_JAVA,
                SSL_FACTORY_LIBPQ,
                SSL_FACTORY_SINGLE_CERT_VALIDATING,
                SSL_FACTORY_LIBPQ_JDBC4
            )
            .defaultValue(SSL_FACTORY_NON_VALIDATING.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor SSL_FACTORY_ARG = new PropertyDescriptor.Builder()
            .name("ssl-factory-arg")
            .displayName("SSL Factory Arg")
            .description("Optional argument passed to the SSL factory class.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor CANCEL_SIGNAL_TIMEOUT = new PropertyDescriptor.Builder()
            .name("cancel-signal-timeout")
            .displayName("Cancel Signal Timeout (seconds)")
            .description("Timeout in seconds for cancel requests.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor RECEIVE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("receive-buffer-size")
            .displayName("Receive Buffer Size (bytes)")
            .description("SO_RCVBUF for the socket.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor SEND_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("send-buffer-size")
            .displayName("Send Buffer Size (bytes)")
            .description("SO_SNDBUF for the socket.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final AllowableValue ESCAPE_SYNTAX_CALL_MODE_SELECT = new AllowableValue(
            "select",
            "Select",
            "Send JDBC escape call syntax as a SELECT."
        );

        public static final AllowableValue ESCAPE_SYNTAX_CALL_MODE_CALL_IF_NO_RETURN = new AllowableValue(
            "callIfNoReturn",
            "Call If No Return",
            "Use CALL if the function returns void; otherwise use SELECT."
        );

        public static final AllowableValue ESCAPE_SYNTAX_CALL_MODE_CALL = new AllowableValue(
            "call",
            "Call",
            "Send JDBC escape call syntax using CALL."
        );

        public static final PropertyDescriptor ESCAPE_SYNTAX_CALL_MODE = new PropertyDescriptor.Builder()
            .name("escape-syntax-call-mode")
            .displayName("Escape Syntax Call Mode")
            .description("Controls how JDBC escape call syntax is sent: select, callIfNoReturn, or call.")
            .allowableValues(
                ESCAPE_SYNTAX_CALL_MODE_SELECT,
                ESCAPE_SYNTAX_CALL_MODE_CALL_IF_NO_RETURN,
                ESCAPE_SYNTAX_CALL_MODE_CALL
            )
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor PREPARE_THRESHOLD = new PropertyDescriptor.Builder()
            .name("prepare-threshold")
            .displayName("Prepare Threshold")
            .description("Number of executions before using server-side prepared statements. 0 disables.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor DEFAULT_ROW_FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("default-row-fetch-size")
            .displayName("Default Row Fetch Size")
            .description("Default number of rows to fetch per network roundtrip. Limiting the number of rows fetched with each trip to the database avoids unnecessary memory consumption and as a consequence OutOfMemoryError. Must be > 0.")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor ADAPTIVE_FETCH = new PropertyDescriptor.Builder()
            .name("adaptive-fetch")
            .displayName("Adaptive Fetch")
            .description("Enable dynamic ResultSet fetch size adjustment (requires Default Row Fetch Size).")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

        public static final PropertyDescriptor MAX_RESULT_BUFFER = new PropertyDescriptor.Builder()
            .name("max-result-buffer")
            .displayName("Max Result Buffer (bytes)")
            .description("Maximum buffer size used when fetching results. Used with Adaptive Fetch.")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
}


