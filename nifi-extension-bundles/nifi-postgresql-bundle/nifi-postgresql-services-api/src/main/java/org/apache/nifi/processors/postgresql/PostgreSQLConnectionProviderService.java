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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processors.postgresql.util.TableMetadata;

/**
 * Controller Service API that provides access to PostgreSQL connections wrapped in {@link PostgreSQLConnectionWrapper} for NiFi components.
 */
public interface PostgreSQLConnectionProviderService extends ControllerService {

    /**
     * Gets a PostgreSQL connection wrapped in a {@link PostgreSQLConnectionWrapper}.
     *
     * @return a PostgreSQLConnectionWrapper instance
     */
    PostgreSQLConnectionWrapper getPostgreSQLConnection();

    /**
     * Gets metadata for a PostgreSQL table including column information, types, primary keys, and JSONB columns.
     * Results are cached to avoid repeated metadata queries.
     *
     * @param schema the schema name
     * @param table  the table name
     * @return TableMetadata instance containing the table metadata
     */
    TableMetadata getTableMetadata(String schema, String table);
}
