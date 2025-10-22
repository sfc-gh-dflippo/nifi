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
 * SQL templates for PostgreSQL upsert operations in ETL pipeline tests.
 * Contains complex CTE-based upsert templates with deduplication and JSONB merge functionality.
 */
public final class PostgreSQLUpsertTemplates {

    private PostgreSQLUpsertTemplates() {
        // Utility class
    }

    /**
     * Complex ETL upsert template with CTE-based deduplication and JSONB merge.
     * This template performs the following operations:
     * 1. deflated_records CTE: Deduplicates records using array_agg with temporal ordering
     * 2. upserted_data CTE: Performs INSERT...ON CONFLICT with JSONB merge
     * 3. Final SELECT: Returns all processed records with proper type casting
     * 
     * Features:
     * - Temporal deduplication using _etl_modified_ timestamps
     * - JSONB merge aggregation in chronological order
     * - Composite primary key conflict resolution (_context_id_, primary_key)
     * - Complete field updates with excluded values
     * - Type casting for consistent output format
     * 
     * @return The complete PostgreSQL upsert SQL template
     */
    public static String getComplexETLUpsertTemplate() {
        return "with deflated_records as (" +
            "    select" +
            "        (array_agg(_etl_run_id_ ORDER BY _etl_modified_ DESC))[1] AS _etl_run_id_," +
            "        (array_agg(_schema_class_ ORDER BY _etl_modified_ DESC))[1] AS _schema_class_," +
            "        _context_id_," +
            "        (array_agg(fulltablename ORDER BY _etl_modified_ DESC))[1] AS fulltablename," +
            "        (array_agg(operation_type ORDER BY _etl_modified_ DESC))[1] AS operation_type," +
            "        (array_agg(name ORDER BY _etl_modified_ DESC))[1] AS name," +
            "        primary_key," +
            "        (array_agg(_is_deleted_ ORDER BY _etl_modified_ DESC))[1] AS _is_deleted_," +
            "        (array_agg(committedtime ORDER BY _etl_modified_ DESC))[1] AS committedtime," +
            "        (array_agg(extractedtime ORDER BY _etl_modified_ DESC))[1] AS extractedtime," +
            "        (array_agg(sortorder ORDER BY _etl_modified_ DESC))[1] AS sortorder," +
            "        (array_agg(loaded_seq ORDER BY _etl_modified_ DESC))[1] AS loaded_seq," +
            "        jsonb_merge_agg(src::jsonb ORDER BY _etl_modified_ ASC) AS src," +
            "        (array_agg(_etl_modified_ ORDER BY _etl_modified_ DESC))[1] AS _etl_modified_," +
            "        (array_agg(_source_extracted_ ORDER BY _etl_modified_ DESC))[1] AS _source_extracted_" +
            "    from ${temp_table} stg" +
            "    group by _context_id_, primary_key" +
            "), upserted_data as (" +
            "    insert into ${target_table}" +
            "    select * from deflated_records" +
            "    on conflict (\"_context_id_\", \"primary_key\") do update set" +
            "        \"_etl_run_id_\" = excluded.\"_etl_run_id_\"," +
            "        \"_schema_class_\" = excluded.\"_schema_class_\"," +
            "        \"fulltablename\" = excluded.\"fulltablename\"," +
            "        \"operation_type\" = excluded.\"operation_type\"," +
            "        \"name\" = excluded.\"name\"," +
            "        \"_is_deleted_\" = excluded.\"_is_deleted_\"," +
            "        \"committedtime\" = excluded.\"committedtime\"," +
            "        \"extractedtime\" = excluded.\"extractedtime\"," +
            "        \"sortorder\" = excluded.\"sortorder\"," +
            "        \"loaded_seq\" = excluded.\"loaded_seq\"," +
            "        \"src\" = ${target_table}.\"src\" || excluded.\"src\"," +
            "        \"_etl_modified_\" = excluded.\"_etl_modified_\"," +
            "        \"_source_extracted_\" = excluded.\"_source_extracted_\"" +
            "    returning *" +
            ")" +
            "select " +
            "    \"_etl_run_id_\"::int8 as \"_etl_run_id_\", " +
            "    \"_schema_class_\"::varchar(255) as \"_schema_class_\", " +
            "    \"_context_id_\"::int8 as \"_context_id_\", " +
            "    \"fulltablename\"::varchar(255) as \"fulltablename\", " +
            "    \"operation_type\"::varchar(50) as \"operation_type\"," +
            "    \"name\"::varchar(255) as \"name\", " +
            "    \"primary_key\"::varchar(255) as \"primary_key\", " +
            "    \"_is_deleted_\"::boolean as \"_is_deleted_\", " +
            "    \"committedtime\"::timestamp as \"committedtime\", " +
            "    \"extractedtime\"::timestamp as \"extractedtime\"," +
            "    \"sortorder\"::int8 as \"sortorder\", " +
            "    \"loaded_seq\"::int8 as \"loaded_seq\", " +
            "    \"src\"::jsonb as \"src\", " +
            "    \"_etl_modified_\"::timestamp as \"_etl_modified_\", " +
            "    \"_source_extracted_\"::timestamp as \"_source_extracted_\"" +
            "from upserted_data";
    }

}
