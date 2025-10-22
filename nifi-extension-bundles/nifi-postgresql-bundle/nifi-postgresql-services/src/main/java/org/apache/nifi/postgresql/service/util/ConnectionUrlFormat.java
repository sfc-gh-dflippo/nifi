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

import org.apache.nifi.components.DescribedValue;

import java.util.Objects;
import java.util.Optional;

/**
 * Enumeration of connection URL construction strategies for PostgreSQL JDBC URLs.
 */
public enum ConnectionUrlFormat implements DescribedValue {
    FULL_URL("full-url", "Full URL", "Provide connection URL in a single property") {
        @Override
        public String buildConnectionUrl(final ConnectionPoolSettings parameters) {
            String postgresqlUrl = parameters.getPostgreSQLUrl();
            if (!postgresqlUrl.startsWith(POSTGRESQL_SCHEME)) {
                postgresqlUrl = POSTGRESQL_URI_PREFIX + postgresqlUrl;
            }

            return postgresqlUrl;
        }
    },
    HOST_NAME("host-name", "Host Name", "Provide a PostgreSQL host name, port and database") {
        @Override
        public String buildConnectionUrl(final ConnectionPoolSettings parameters) {
            final String hostName = Objects.requireNonNull(parameters.getHostName());
            final String port = Objects.requireNonNull(parameters.getPort());
            final String database = Objects.requireNonNull(parameters.getDatabase());
            return POSTGRESQL_URI_PREFIX + hostName + ":" + port + "/" + database;
        }
    }
    ;

    public static final String POSTGRESQL_SCHEME = "jdbc:postgresql";
    public static final String POSTGRESQL_URI_PREFIX = POSTGRESQL_SCHEME + "://";

    private final String value;
    private final String displayName;
    private final String description;

    ConnectionUrlFormat(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public abstract String buildConnectionUrl(final ConnectionPoolSettings parameters);

}
