<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

### PostgreSQL Bulk Upsert

#### Overview

The PostgreSQL Bulk Upsert processor performs high-performance upsert (INSERT ... ON CONFLICT) operations by staging data into a temporary table using PostgreSQL's `COPY FROM STDIN` protocol, then executing a configurable upsert SQL template to merge data into the target table. This approach provides optimal performance for bulk data synchronization scenarios.

The processor supports both CSV and Parquet input formats, with optional result export capabilities. It leverages PostgreSQL's native COPY protocol for maximum throughput while providing flexible upsert logic through SQL templates.

#### Key Features

- **High Performance**: Uses PostgreSQL COPY protocol for optimal bulk data transfer
- **Flexible Upsert Logic**: Configurable SQL templates for custom conflict resolution
- **Multiple Data Formats**: Supports CSV (via RecordReader) and Parquet input
- **Result Export**: Optional export of upsert results via COPY TO STDOUT
- **Transaction Safety**: Automatic transaction management with rollback on failure
- **Identifier Quoting**: Safe handling of PostgreSQL identifiers and reserved words

#### Data Flow Process

1. **Staging**: Creates temporary table and loads data via COPY FROM STDIN
2. **Upsert**: Executes configured INSERT ... ON CONFLICT SQL template
3. **Export** (Optional): Exports results via COPY (query) TO STDOUT
4. **Cleanup**: Drops temporary table and commits transaction

#### Configuration Properties

| Property | Required | Description |
|----------|----------|-------------|
| PostgreSQL Connection Provider | Yes | Connection pool service for PostgreSQL database |
| Target Table | Yes | Destination table for upsert operations |
| Upsert SQL Template | Yes | SQL template with placeholders for staging and target tables |
| Record Reader | Yes | Service for parsing incoming FlowFile records |
| CSV Record Writer | Yes | Service for generating COPY CSV format |
| Data Format | No | Input format: CSV (default) or Parquet |
| Result SQL | No | Optional query for exporting upsert results |
| Record Writer | No | Service for writing result records (required if Result SQL specified) |
| Result Data Format | No | Output format for results: CSV (default) or Parquet |

#### CSV Configuration

The processor uses PostgreSQL's default CSV format for COPY operations:

| Setting | Default Value | Description |
|---------|---------------|-------------|
| Delimiter | `,` (comma) | Field separator character |
| Quote Character | `"` (double quote) | Character for quoting fields |
| Escape Character | `"` (double quote) | Character for escaping quotes |
| NULL Representation | Empty string | How NULL values are represented |

#### Upsert SQL Template

The SQL template supports the following placeholders:

- `{temp_table}`: Name of the temporary staging table
- `{target_table}`: Name of the target table
- `{columns}`: Comma-separated list of column names

**Example Template:**
```sql
INSERT INTO {target_table} 
SELECT * FROM {temp_table}
ON CONFLICT (id) 
DO UPDATE SET 
    name = EXCLUDED.name,
    updated_at = NOW()
```

#### Performance Characteristics

- **Native COPY Protocol:** Leverages PostgreSQL's optimized bulk operations for maximum throughput
- **Flexible Format Support:** Both CSV and Parquet input formats with streaming processing
- **Efficient Staging:** Temporary table staging minimizes overhead
- **Transaction Safe:** Automatic rollback ensures data consistency

#### Data Type Support

The processor supports all PostgreSQL data types through appropriate RecordReader/Writer configurations:

- **Numeric Types**: INTEGER, BIGINT, DECIMAL, NUMERIC, REAL, DOUBLE PRECISION
- **Text Types**: VARCHAR, TEXT, CHAR, with proper escaping
- **Date/Time Types**: DATE, TIME, TIMESTAMP, TIMESTAMPTZ
- **JSON Types**: JSON, JSONB with proper formatting
- **Binary Types**: BYTEA (Base64 encoded)
- **Array Types**: All array types with proper formatting

#### Error Handling

The processor implements comprehensive error handling:

- **Connection Failures**: Automatic retry with connection pool
- **SQL Errors**: Detailed error messages with SQL context
- **Data Type Mismatches**: Clear validation error messages
- **Transaction Rollback**: Automatic cleanup on any failure
- **Temporary Table Cleanup**: Guaranteed cleanup even on errors

#### Security Considerations

- **SQL Injection Protection**: Parameterized queries and identifier quoting
- **Connection Security**: SSL/TLS support via connection pool configuration
- **Credential Management**: Secure credential handling through NiFi services
- **Access Control**: Respects PostgreSQL user permissions and row-level security

**Environment Variable Integration:**
```bash
export PGHOST=mydb.example.com
export PGUSER=app_user
export PGPASSWORD=secure_password
export PGSSLMODE=verify-full
```
Connection pool can reference these through NiFi expression language for secure credential management.

#### Usage Examples

**Basic Upsert:**
```sql
INSERT INTO users (id, name, email, updated_at)
SELECT id, name, email, NOW()
FROM {temp_table}
ON CONFLICT (id) 
DO UPDATE SET 
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    updated_at = EXCLUDED.updated_at
```

**Conditional Upsert:**
```sql
INSERT INTO products (sku, name, price, version)
SELECT sku, name, price, version
FROM {temp_table}
ON CONFLICT (sku)
DO UPDATE SET 
    name = EXCLUDED.name,
    price = EXCLUDED.price,
    version = EXCLUDED.version
WHERE products.version < EXCLUDED.version
```

**JSONB Merge Pattern:**
```sql
INSERT INTO users (id, profile_data, updated_at)
SELECT id, profile_data, NOW()
FROM {temp_table}
ON CONFLICT (id) 
DO UPDATE SET 
    profile_data = users.profile_data || EXCLUDED.profile_data,
    updated_at = EXCLUDED.updated_at
```
*Uses PostgreSQL's `||` operator to merge JSONB objects, preserving existing keys while adding new ones*

**JSONB Conditional Merge:**
```sql
INSERT INTO products (sku, metadata, version)
SELECT sku, metadata, version
FROM {temp_table}
ON CONFLICT (sku)
DO UPDATE SET 
    metadata = CASE 
        WHEN EXCLUDED.version > products.version 
        THEN products.metadata || EXCLUDED.metadata
        ELSE products.metadata
    END,
    version = GREATEST(products.version, EXCLUDED.version)
```

**Upsert with Result Export:**
- Upsert SQL: Standard INSERT ... ON CONFLICT
- Result SQL: `SELECT * FROM {target_table} WHERE updated_at > NOW() - INTERVAL '1 minute'`

#### Integration Patterns

**ETL Pipeline:**
1. Extract data from source systems
2. Transform using NiFi processors
3. Upsert into PostgreSQL staging tables
4. Export results for downstream processing

**Change Data Capture:**
1. Receive CDC events from source databases
2. Apply business logic transformations
3. Upsert changes to target tables
4. Export audit trail of changes

**Data Synchronization:**
1. Batch extract from multiple sources
2. Merge and deduplicate records
3. Bulk upsert to maintain data consistency
4. Export synchronization status

#### Monitoring and Troubleshooting

**Key Metrics:**
- `record.count`: Number of records processed
- Processing time via NiFi provenance
- Connection pool utilization
- PostgreSQL query performance

**Quick Troubleshooting:**

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Slow performance | Small batch sizes or connection pool exhausted | Increase batch size; increase Max Total Connections |
| Memory errors | Dataset too large | Reduce batch size; monitor temporary table size |
| Deadlocks | Lock contention in upsert SQL | Review conflict columns and index strategy |
| Data type errors | Schema mismatch | Verify RecordReader schema matches table structure |
| SSL/TLS errors | Certificate issues | Check SSL mode and certificate paths |
| Connection timeout | Pool exhausted or database overload | Increase Max Wait Time; check database load |

**Debug Logging:** Set `org.apache.nifi.processors.postgresql` to DEBUG level in logback.xml

#### Best Practices

1. **Batch Sizing**: Optimize FlowFile sizes (1,000-10,000 records typical)
2. **Connection Pooling**: Configure appropriate pool sizes for concurrent processing
3. **Index Strategy**: Ensure target tables have appropriate indexes for conflict detection
4. **Monitoring**: Implement comprehensive monitoring of throughput and error rates
5. **Testing**: Validate upsert logic with representative data samples
6. **Schema Evolution**: Plan for schema changes in both source and target systems

#### References

- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [PostgreSQL INSERT ON CONFLICT](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
- [NiFi RecordReader/Writer Services](https://nifi.apache.org/docs/nifi-docs/html/record-path-guide.html)


