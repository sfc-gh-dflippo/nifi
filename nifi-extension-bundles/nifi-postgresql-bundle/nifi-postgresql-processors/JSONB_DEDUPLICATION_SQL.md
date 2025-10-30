# PostgreSQL JSONB Deduplication and Merge Patterns

## Overview

This document explains how the **PostgreSQLBulkUpsert** processor's deduplication function works for **JSONB** and **scalar** columns when the `Enable Deduplication` property is enabled.

### Key Concepts

The processor implements two distinct deduplication strategies based on column type:

**For scalar columns (VARCHAR, INT, TIMESTAMP, etc.):**
- Uses `array_agg()` to keep the most recent value ordered by the deduplication timestamp column

**For JSONB columns:**
- Uses the `jsonb_merge_agg()` custom aggregate to merge all versions chronologically
- Older values are applied first, newer values override them
- Automatically creates the `jsonb_merge_agg()` function if it doesn't exist

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

## Three Deduplication Strategies Based on Column Type

When deduplication is enabled, the processor applies different aggregation strategies based on the column type:

### 1. Primary Key Columns ✅
- **No aggregation** - these columns are used in the `GROUP BY` clause
- They define the unique record identity
- Example: `primary_key`, `id`, composite keys like `(_context_id_, primary_key)`

### 2. Scalar Columns (VARCHAR, INT, TIMESTAMP, etc.) 📝
- **Keep the MOST RECENT value** using `ORDER BY timestamp_column DESC`
- Uses PostgreSQL's `array_agg()` with `[1]` to select the first (most recent) element
- This ensures the latest update "wins" for simple data types
- Example SQL:
  ```sql
  (array_agg(name ORDER BY _etl_modified_ DESC))[1] AS name
  ```

### 3. JSONB/JSON Columns 🔗
- **Merge ALL versions chronologically** using `ORDER BY timestamp_column ASC`
- Uses the custom `jsonb_merge_agg()` aggregate function (created automatically)
- Older values are applied first, then newer values override them progressively
- This preserves the complete evolution of the JSONB document
- Example SQL:
  ```sql
  jsonb_merge_agg(src ORDER BY _etl_modified_ ASC) AS src
  ```

---

## Example Scenario: Two-Level Deduplication and Merge

This example demonstrates the complete deduplication and merge process, including both:
1. **Level 1**: Deduplication within the temporary table (multiple updates in the same batch)
2. **Level 2**: Merging with existing data in the target table during upsert

### Initial State: Existing Data in Target Table

The target table already has a record for `primary_key = 123`:

```
primary_key | name      | src                           | _etl_modified_
-----------+----------+-------------------------------+-------------------
123        | "Jane"    | {"phone": "555-1234"}         | 2023-12-15 09:00
```

### Incoming Data in Temporary Table

You receive multiple updates for the same record (`primary_key = 123`) in a single batch:

```
primary_key | name     | src                    | _etl_modified_
-----------+---------+------------------------+-------------------
123        | "John"   | {"age": 25}            | 2024-01-01 10:00
123        | "John"   | {"city": "NYC"}        | 2024-01-01 11:00  
123        | "Johnny" | {"age": 26}            | 2024-01-01 12:00
```

### Step 1: Deduplication Within Temporary Table

The processor groups by `primary_key` and deduplicates the 3 rows:

```
primary_key | name     | src                              | _etl_modified_
-----------+---------+----------------------------------+-------------------
123        | "Johnny" | {"age": 26, "city": "NYC"}       | 2024-01-01 12:00
```

**What happened during temp table deduplication:**
- **Scalar `name` field**: Picked most recent → `"Johnny"` (DESC ordered)
- **JSONB `src` field**: Merged all versions chronologically (ASC ordered):
  - `{} || {"age": 25} || {"city": "NYC"} || {"age": 26}`
  - Result: `{"age": 26, "city": "NYC"}`
- **Timestamp**: Kept most recent → `2024-01-01 12:00`

### Step 2: Upsert with Target Table JSONB Merge

The deduplicated record is then upserted into the target table. For JSONB columns, the upsert uses concatenation:

```sql
"src" = target_table."src" || excluded."src"
```

**JSONB merge during upsert:**
```
Existing:      {"phone": "555-1234"}                    -- From target table
New:           {"age": 26, "city": "NYC"}               -- From deduplicated temp
Final result:  {"phone": "555-1234", "age": 26, "city": "NYC"}  -- Merged!
```

### Final Result in Target Table

After the complete upsert operation:

```
primary_key | name     | src                                           | _etl_modified_
-----------+---------+-----------------------------------------------+-------------------
123        | "Johnny" | {"phone": "555-1234", "age": 26, "city": "NYC"} | 2024-01-01 12:00
```

### Complete Transformation Summary

**Scalar field (`name`):**
- Old value in target: `"Jane"`
- New value from temp (after dedup): `"Johnny"`
- **Final**: `"Johnny"` ✅ (new value replaces old)

**JSONB field (`src`):**
- Old value in target: `{"phone": "555-1234"}`
- Incoming updates (3 rows): `{"age": 25}`, `{"city": "NYC"}`, `{"age": 26}`
- After temp dedup: `{"age": 26, "city": "NYC"}`
- **Final**: `{"phone": "555-1234", "age": 26, "city": "NYC"}` ✅ (complete merge preserving all keys)

**Timestamp (`_etl_modified_`):**
- Old: `2023-12-15 09:00`
- New: `2024-01-01 12:00`
- **Final**: `2024-01-01 12:00` ✅ (most recent)

### Key Insights

This two-level merge approach ensures:
- ✅ **Within batch**: Multiple updates to the same record are properly merged
- ✅ **Across batches**: Historical JSONB data is preserved (`phone` field retained)
- ✅ **Scalar fields**: Always reflect the most recent value
- ✅ **JSONB fields**: Accumulate changes over time, perfect for CDC scenarios
- ✅ **Data consistency**: All related fields stay in sync with the most recent timestamp

---

## SQL Implementation Details

The following sections document the SQL patterns used internally by the processor. **You don't need to write this SQL yourself** - the processor generates it automatically.

---

## How the Processor Works: Complete ETL Flow with JSONB Deduplication

### Process Overview
When deduplication is enabled, the processor executes the following steps:
1. Loads incoming data into a temporary table
2. Aggregates multiple records for the same primary key (or composite key)
3. Uses the specified timestamp column to merge JSONB chronologically
4. Applies the `jsonb_merge_agg()` custom aggregate for JSONB columns
5. Performs upsert into the target table with proper JSONB field merging

---

## SQL Deduplication Implementation

The processor generates different SQL aggregation logic based on column type:

### Scalar Columns (VARCHAR, INT, TIMESTAMP, etc.)

The processor uses `array_agg()` with `ORDER BY timestamp DESC` and selects the first element `[1]` to keep the most recent value:

```sql
(array_agg(name ORDER BY _etl_modified_ DESC))[1] AS name
(array_agg(_etl_run_id_ ORDER BY _etl_modified_ DESC))[1] AS _etl_run_id_
(array_agg(operation_type ORDER BY _etl_modified_ DESC))[1] AS operation_type
```

### JSONB Columns

The processor uses the `jsonb_merge_agg()` custom aggregate function with `ORDER BY timestamp ASC` to merge all versions chronologically:

```sql
jsonb_merge_agg(src ORDER BY _etl_modified_ ASC) AS src
```

### The jsonb_merge_agg Aggregate Function

The processor automatically creates this custom aggregate if it doesn't exist in your database:

```sql
CREATE AGGREGATE jsonb_merge_agg(jsonb) (
    SFUNC = jsonb_concat,     -- State transition function (same as || operator)
    STYPE = jsonb,            -- State type
    INITCOND = '{}'           -- Initial state: empty JSONB object
);
```

**How it works:**
1. Starts with an empty JSONB object `{}`
2. For each row (ordered by timestamp ASC), applies `result || new_value`
3. Newer values override older values (chronological merge)
4. Result contains all keys from all versions, with latest values winning

---

## Key Takeaways

1. **Scalar fields**: The processor uses `array_agg()` with `DESC` ordering to keep the most recent value
2. **JSONB fields**: The processor uses `jsonb_merge_agg()` with `ASC` ordering for chronological merge
3. **JSONB concatenation (||)**: Right side overwrites left side (newer values win)
4. **Automatic setup**: The `jsonb_merge_agg` aggregate is created automatically if needed
5. **Two-level merging**: First within temp table (deduplication), then with target table (upsert)
6. **Primary keys**: Used for `GROUP BY` during deduplication

---

## Complete Generated SQL Example

This is the actual SQL generated by the processor for a typical upsert with deduplication:

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

