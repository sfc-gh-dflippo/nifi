# PostgreSQL JSONB Deduplication and Merge Patterns

## Overview
This document describes advanced SQL patterns for deduplicating and merging JSONB data in PostgreSQL using:
- `array_agg()` for grouping by primary key
- `jsonb_merge_agg()` custom aggregate for merging JSONB objects chronologically
- JSONB concatenation (`||`) for merging
- Ordering by timestamp columns like `_etl_modified_` for chronological merging

These patterns are **automatically implemented** in the **PostgreSQLBulkUpsert** processor when the `Enable Deduplication` property is set to `true`.

---

## Using Deduplication in NiFi

### PostgreSQLBulkUpsert Processor Configuration

The `PostgreSQLBulkUpsert` processor now includes built-in support for deduplication:

1. **Enable Deduplication** (boolean, default: false)
   - When enabled, deduplicates records in the temporary table before upserting
   - Groups records by primary key columns
   - Uses the Deduplication Timestamp Column for ordering

2. **Deduplication Timestamp Column** (string, required when deduplication enabled)
   - Column name to use for ordering records chronologically
   - Should contain a timestamp or other sortable value
   - Examples: `_etl_modified_`, `updated_at`, `timestamp`

### How It Works

When deduplication is enabled:
1. Records are grouped by the table's primary key columns
2. For **scalar fields**: The processor picks the most recent value (ordered DESC by timestamp)
3. For **JSONB/JSON fields**: The processor merges all versions chronologically (ordered ASC by timestamp) using the `jsonb_merge_agg()` aggregate function
4. The `jsonb_merge_agg()` function is **automatically created** if it doesn't already exist in your database
5. The deduplicated records are then upserted into the target table with JSONB concatenation for conflict resolution

### Example Configuration

```
Connection Provider: PostgreSQL-Connection-Pool
Target Table: public.my_table
Data Format: Parquet
Enable Deduplication: true
Deduplication Timestamp Column: _etl_modified_
Upsert Returns Records: true
```

This configuration will:
- Load incoming data into a temporary table
- Deduplicate records by primary key, merging JSONB fields chronologically
- Upsert the deduplicated records into `public.my_table`
- Return the upserted records with JSONB cast to TEXT for Parquet compatibility

---

## SQL Implementation Details

The following sections document the SQL patterns used internally by the processor. **You don't need to write this SQL yourself** - the processor generates it automatically.

---

## Complex ETL Upsert Template with JSONB Aggregation and Merging

### Purpose
This template demonstrates how to:
1. Load data into a temporary table
2. Aggregate multiple records for the same `(_context_id_, primary_key)` composite key
3. Use `_etl_modified_` timestamp ordering to merge JSONB chronologically
4. Leverage `jsonb_merge_agg()` custom aggregate or array_agg patterns
5. Perform upsert into target table with proper JSONB field merging

---

## Pattern 1: Using array_agg for Aggregation-Based JSONB Merging

### Concept
Use `array_agg()` with ordering to collect values from multiple rows for the same primary key. For scalar fields, use `[1]` to pick the most recent value. For JSONB fields, this approach sets up the foundation for proper merging.

```sql
WITH aggregated AS (
    SELECT
        _context_id_,
        primary_key,
        (array_agg(_etl_run_id_ ORDER BY _etl_modified_ DESC))[1] AS _etl_run_id_,
        (array_agg(_schema_class_ ORDER BY _etl_modified_ DESC))[1] AS _schema_class_,
        (array_agg(fulltablename ORDER BY _etl_modified_ DESC))[1] AS fulltablename,
        (array_agg(operation_type ORDER BY _etl_modified_ DESC))[1] AS operation_type,
        (array_agg(name ORDER BY _etl_modified_ DESC))[1] AS name,
        (array_agg(_is_deleted_ ORDER BY _etl_modified_ DESC))[1] AS _is_deleted_,
        (array_agg(committedtime ORDER BY _etl_modified_ DESC))[1] AS committedtime,
        (array_agg(extractedtime ORDER BY _etl_modified_ DESC))[1] AS extractedtime,
        (array_agg(sortorder ORDER BY _etl_modified_ DESC))[1] AS sortorder,
        (array_agg(loaded_seq ORDER BY _etl_modified_ DESC))[1] AS loaded_seq,
        -- For JSONB, we could create a custom aggregate that merges all versions
        (array_agg(src ORDER BY _etl_modified_ ASC))[array_length(array_agg(src ORDER BY _etl_modified_ ASC), 1)] AS src,
        max(_etl_modified_) AS _etl_modified_,
        (array_agg(_source_extracted_ ORDER BY _etl_modified_ DESC))[1] AS _source_extracted_
    FROM ${temp_table}
    GROUP BY _context_id_, primary_key
)
SELECT * FROM aggregated;
```

---

## Pattern 2: Using jsonb_merge_agg Custom Aggregate (Recommended)

### Purpose
Create a PostgreSQL aggregate function that properly merges multiple JSONB objects in chronological order, allowing newer values to override older ones:

```sql
-- Hypothetical usage:
SELECT
    _context_id_,
    primary_key,
    jsonb_merge_agg(src ORDER BY _etl_modified_ ASC) AS merged_src
FROM ${temp_table}
GROUP BY _context_id_, primary_key;
```

### Implementation Strategy
The custom aggregate would:
1. Start with an empty JSONB object `{}`
2. For each row (ordered by timestamp), apply `result || new_value`
3. This ensures newer values override older values in the merge

### PostgreSQL Lower-Level Function
The `||` operator for JSONB uses the internal `jsonb_concat()` function. A custom aggregate could leverage this:

```sql
CREATE AGGREGATE jsonb_merge_agg(jsonb) (
    SFUNC = jsonb_concat,     -- State transition function (same as || operator)
    STYPE = jsonb,            -- State type
    INITCOND = '{}'           -- Initial state: empty JSONB object
);
```

### Benefits
1. **Performance**: Single pass through data instead of multiple array aggregations
2. **Clarity**: Clear intent of merging JSONB objects
3. **Flexibility**: Can handle complex merge scenarios
4. **Efficiency**: Leverages native PostgreSQL JSONB operations

---

## Key Takeaways

1. **array_agg()** provides flexibility for collecting values from multiple rows
2. **JSONB concatenation (||)** has right-side precedence (right overwrites left)
3. **Custom jsonb_merge_agg aggregate** is the proper solution for merging JSONB from multiple records
4. **Ordering matters**: Use `ASC` for chronological merge (older → newer) so final values win
5. **Grouping by primary key** is essential for aggregation-based approaches
6. **Two-level merging**: Merge within temp table (via aggregate), then merge with target table (via upsert)

---

## Complete Working Example from PostgreSQLUpsertTemplates.java

```sql
WITH deflated_records AS (
    SELECT
        (array_agg(_etl_run_id_ ORDER BY _etl_modified_ DESC))[1] AS _etl_run_id_,
        (array_agg(_schema_class_ ORDER BY _etl_modified_ DESC))[1] AS _schema_class_,
        _context_id_,
        (array_agg(fulltablename ORDER BY _etl_modified_ DESC))[1] AS fulltablename,
        (array_agg(operation_type ORDER BY _etl_modified_ DESC))[1] AS operation_type,
        (array_agg(name ORDER BY _etl_modified_ DESC))[1] AS name,
        primary_key,
        (array_agg(_is_deleted_ ORDER BY _etl_modified_ DESC))[1] AS _is_deleted_,
        (array_agg(committedtime ORDER BY _etl_modified_ DESC))[1] AS committedtime,
        (array_agg(extractedtime ORDER BY _etl_modified_ DESC))[1] AS extractedtime,
        (array_agg(sortorder ORDER BY _etl_modified_ DESC))[1] AS sortorder,
        (array_agg(loaded_seq ORDER BY _etl_modified_ DESC))[1] AS loaded_seq,
        -- JSONB merge: Use custom aggregate ordered chronologically (ASC)
        jsonb_merge_agg(src::jsonb ORDER BY _etl_modified_ ASC) AS src,
        (array_agg(_etl_modified_ ORDER BY _etl_modified_ DESC))[1] AS _etl_modified_,
        (array_agg(_source_extracted_ ORDER BY _etl_modified_ DESC))[1] AS _source_extracted_
    FROM ${temp_table}
    GROUP BY _context_id_, primary_key
),
upserted_data AS (
    INSERT INTO ${target_table}
    SELECT * FROM deflated_records
    ON CONFLICT ("_context_id_", "primary_key") DO UPDATE SET
        "_etl_run_id_" = excluded."_etl_run_id_",
        "_schema_class_" = excluded."_schema_class_",
        "fulltablename" = excluded."fulltablename",
        "operation_type" = excluded."operation_type",
        "name" = excluded."name",
        "_is_deleted_" = excluded."_is_deleted_",
        "committedtime" = excluded."committedtime",
        "extractedtime" = excluded."extractedtime",
        "sortorder" = excluded."sortorder",
        "loaded_seq" = excluded."loaded_seq",
        -- Final JSONB merge: existing || new (new values override)
        "src" = ${target_table}."src" || excluded."src",
        "_etl_modified_" = excluded."_etl_modified_",
        "_source_extracted_" = excluded."_source_extracted_"
    RETURNING *
)
SELECT
    "_etl_run_id_"::int8 AS "_etl_run_id_",
    "_schema_class_"::varchar(255) AS "_schema_class_",
    "_context_id_"::int8 AS "_context_id_",
    "fulltablename"::varchar(255) AS "fulltablename",
    "operation_type"::varchar(50) AS "operation_type",
    "name"::varchar(255) AS "name",
    "primary_key"::varchar(255) AS "primary_key",
    "_is_deleted_"::boolean AS "_is_deleted_",
    "committedtime"::timestamp AS "committedtime",
    "extractedtime"::timestamp AS "extractedtime",
    "sortorder"::int8 AS "sortorder",
    "loaded_seq"::int8 AS "loaded_seq",
    "src"::jsonb AS "src",
    "_etl_modified_"::timestamp AS "_etl_modified_",
    "_source_extracted_"::timestamp AS "_source_extracted_"
FROM upserted_data;
```

---

## References
- PostgreSQL JSONB Documentation: https://www.postgresql.org/docs/current/datatype-json.html
- Create Aggregate: https://www.postgresql.org/docs/current/sql-createaggregate.html
- JSONB Operators: https://www.postgresql.org/docs/current/functions-json.html
- Array Aggregates: https://www.postgresql.org/docs/current/functions-aggregate.html

