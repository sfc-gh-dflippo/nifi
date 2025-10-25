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

package org.apache.nifi.processors.postgresql.integration.sql;

/**
 * SQL templates for PostgreSQL upsert operations in ETL pipeline tests. Contains complex CTE-based upsert templates with deduplication and JSONB
 * merge functionality.
 */
public final class PostgreSQLUpsertTemplates {

    private PostgreSQLUpsertTemplates() {
        // Utility class
    }

    /**
     * Complex ETL upsert template with CTE-based deduplication and JSONB merge. This template performs the following operations: 1. deflated_records
     * CTE: Deduplicates records using array_agg with temporal ordering 2. upserted_data CTE: Performs INSERT...ON CONFLICT with JSONB merge 3. Final
     * SELECT: Returns all processed records with proper type casting
     *
     * Features: - Temporal deduplication using _etl_modified_ timestamps - JSONB merge aggregation in chronological order - Composite primary key
     * conflict resolution (_context_id_, primary_key) - Complete field updates with excluded values - Type casting for consistent output format
     *
     * @return The complete PostgreSQL upsert SQL template
     */
    public static String getComplexETLUpsertTemplate() {
        return "WITH deflated_records AS (" + "\n" + "    SELECT" + "\n"
                + "        (array_agg(_etl_run_id_ ORDER BY _etl_modified_ DESC))[1] AS _etl_run_id_," + "\n"
                + "        (array_agg(_schema_class_ ORDER BY _etl_modified_ DESC))[1] AS _schema_class_," + "\n" + "        _context_id_," + "\n"
                + "        (array_agg(fulltablename ORDER BY _etl_modified_ DESC))[1] AS fulltablename," + "\n"
                + "        (array_agg(operation_type ORDER BY _etl_modified_ DESC))[1] AS operation_type," + "\n"
                + "        (array_agg(name ORDER BY _etl_modified_ DESC))[1] AS name," + "\n" + "        primary_key," + "\n"
                + "        (array_agg(_is_deleted_ ORDER BY _etl_modified_ DESC))[1] AS _is_deleted_," + "\n"
                + "        (array_agg(committedtime ORDER BY _etl_modified_ DESC))[1] AS committedtime," + "\n"
                + "        (array_agg(extractedtime ORDER BY _etl_modified_ DESC))[1] AS extractedtime," + "\n"
                + "        (array_agg(sortorder ORDER BY _etl_modified_ DESC))[1] AS sortorder," + "\n"
                + "        (array_agg(loaded_seq ORDER BY _etl_modified_ DESC))[1] AS loaded_seq," + "\n"
                + "        jsonb_merge_agg(src::jsonb ORDER BY _etl_modified_ ASC) AS src," + "\n"
                + "        (array_agg(_etl_modified_ ORDER BY _etl_modified_ DESC))[1] AS _etl_modified_," + "\n"
                + "        (array_agg(_source_extracted_ ORDER BY _etl_modified_ DESC))[1] AS _source_extracted_" + "\n"
                + "    FROM ${temp_table} stg" + "\n" + "    GROUP BY _context_id_, primary_key" + "\n" + "), upserted_data AS (" + "\n"
                + "    INSERT INTO ${target_table}" + "\n" + "    SELECT * FROM deflated_records" + "\n"
                + "    ON CONFLICT (\"_context_id_\", \"primary_key\") DO UPDATE SET" + "\n" + "        \"_etl_run_id_\" = excluded.\"_etl_run_id_\","
                + "\n" + "        \"_schema_class_\" = excluded.\"_schema_class_\"," + "\n"
                + "        \"fulltablename\" = excluded.\"fulltablename\"," + "\n" + "        \"operation_type\" = excluded.\"operation_type\","
                + "\n" + "        \"name\" = excluded.\"name\"," + "\n" + "        \"_is_deleted_\" = excluded.\"_is_deleted_\"," + "\n"
                + "        \"committedtime\" = excluded.\"committedtime\"," + "\n" + "        \"extractedtime\" = excluded.\"extractedtime\"," + "\n"
                + "        \"sortorder\" = excluded.\"sortorder\"," + "\n" + "        \"loaded_seq\" = excluded.\"loaded_seq\"," + "\n"
                + "        \"src\" = ${target_table}.\"src\" || excluded.\"src\"," + "\n"
                + "        \"_etl_modified_\" = excluded.\"_etl_modified_\"," + "\n"
                + "        \"_source_extracted_\" = excluded.\"_source_extracted_\"" + "\n" + "    RETURNING *" + "\n" + ")" + "\n" + "SELECT" + "\n"
                + "    \"_etl_run_id_\"::int8 AS \"_etl_run_id_\"," + "\n" + "    \"_schema_class_\"::varchar(255) AS \"_schema_class_\"," + "\n"
                + "    \"_context_id_\"::int8 AS \"_context_id_\"," + "\n" + "    \"fulltablename\"::varchar(255) AS \"fulltablename\"," + "\n"
                + "    \"operation_type\"::varchar(50) AS \"operation_type\"," + "\n" + "    \"name\"::varchar(255) AS \"name\"," + "\n"
                + "    \"primary_key\"::varchar(255) AS \"primary_key\"," + "\n" + "    \"_is_deleted_\"::boolean AS \"_is_deleted_\"," + "\n"
                + "    \"committedtime\"::timestamp AS \"committedtime\"," + "\n" + "    \"extractedtime\"::timestamp AS \"extractedtime\"," + "\n"
                + "    \"sortorder\"::int8 AS \"sortorder\"," + "\n" + "    \"loaded_seq\"::int8 AS \"loaded_seq\"," + "\n"
                + "    \"src\"::jsonb AS \"src\"," + "\n" + "    \"_etl_modified_\"::timestamp AS \"_etl_modified_\"," + "\n"
                + "    \"_source_extracted_\"::timestamp AS \"_source_extracted_\"" + "\n" + "FROM upserted_data";
    }

}
