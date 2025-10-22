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

import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Thin wrapper around a JDBC {@link java.sql.Connection} that exposes
 * PostgreSQL-specific {@link org.postgresql.PGConnection} unwrapping
 * and implements {@link AutoCloseable} for try-with-resources usage.
 */
public class PostgreSQLConnectionWrapper implements AutoCloseable {

    final Connection connection;

    public PostgreSQLConnectionWrapper(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    /**
     * Gets the underlying JDBC connection.
     * @return the wrapped JDBC connection
     */
    public Connection getConnection() {
        return connection;
    }

    public PGConnection unwrap() throws SQLException {
        return connection.unwrap(PGConnection.class);
    }
}
