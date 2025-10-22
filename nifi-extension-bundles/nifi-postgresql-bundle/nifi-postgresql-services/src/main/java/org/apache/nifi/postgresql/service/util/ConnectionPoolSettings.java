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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processors.postgresql.util.ConnectionSettings;

public class ConnectionPoolSettings {

    public static final PropertyDescriptor CONNECTION_URL_FORMAT = new PropertyDescriptor.Builder()
            .name("connection-url-format")
            .displayName("Connection URL Format")
            .description("The format of the connection URL.")
            .allowableValues(ConnectionUrlFormat.class)
            .required(true)
            .defaultValue(ConnectionUrlFormat.FULL_URL)
            .build();

    public static final PropertyDescriptor POSTGRESQL_URL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DBCPProperties.DATABASE_URL)
            .displayName("PostgreSQL URL")
            .description("Example connection string: jdbc:postgresql://[host]:[port]/[database]/?[connection_params] The connection parameters can include currentSchema=SCHEMA_NAME to avoid using qualified table names such as SCHEMA_NAME.TABLE_NAME")
            .required(true)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.FULL_URL)
            .build();

    // Connection properties decorated with conditional visibility based on URL format
    // Base definitions from ConnectionSettings, only adding .dependsOn() for service-specific behavior
    public static final PropertyDescriptor POSTGRESQL_HOST_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ConnectionSettings.HOST_NAME)
            .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.HOST_NAME)
            .build();

    public static final PropertyDescriptor POSTGRESQL_PORT = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(ConnectionSettings.PORT)
        .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.HOST_NAME)
        .build();

    public static final PropertyDescriptor POSTGRESQL_DATABASE = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(ConnectionSettings.DATABASE)
        .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.HOST_NAME)
        .build();

    public static final PropertyDescriptor POSTGRESQL_SCHEMA = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(ConnectionSettings.SCHEMA)
        .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.HOST_NAME)
        .build();

    public static final PropertyDescriptor POSTGRESQL_SSL_MODE = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(ConnectionSettings.SSL_MODE)
        .dependsOn(CONNECTION_URL_FORMAT, ConnectionUrlFormat.HOST_NAME)
        .build();

    public static final PropertyDescriptor POSTGRESQL_USER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DBCPProperties.DB_USER)
            .displayName("Username")
            .description("The PostgreSQL user name.")
            .build();

    public static final PropertyDescriptor POSTGRESQL_PASSWORD = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DBCPProperties.DB_PASSWORD)
            .displayName("Password")
            .description("The password for the PostgreSQL user.")
            .build();

    protected final String postgresqlUrl;
    protected final String hostName;
    protected final String port;
    protected final String database;
    protected final String schema;
    protected final String ssl;
    protected final String sslMode;
    protected final String sslCertificate;
    protected final String sslKey;
    protected final String sslRootCertificate;

    public ConnectionPoolSettings(
            final String postgresqlUrl,
            final String hostName,
            final String port,
            final String database,
            final String schema,
            final String ssl,
            final String sslMode,
            final String sslCertificate,
            final String sslKey,
            final String sslRootCertificate) {
        this.postgresqlUrl = postgresqlUrl;
        this.hostName = hostName;
        this.port = port;
        this.database = database;
        this.schema = schema;
        this.ssl = ssl;
        this.sslMode = sslMode;
        this.sslCertificate = sslCertificate;
        this.sslKey = sslKey;
        this.sslRootCertificate = sslRootCertificate;
    }

    public String getPostgreSQLUrl() {
        return postgresqlUrl;
    }

    public String getHostName() {
        return hostName;
    }

    public String getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getSsl() {
        return ssl;
    }

    public String getSslMode() {
        return sslMode;
    }

    public String getSslCertificate() {
        return sslCertificate;
    }

    public String getSslKey() {
        return sslKey;
    }

    public String getSslRootCertificate() {
        return sslRootCertificate;
    }
}


