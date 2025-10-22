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

### PostgreSQL Bulk Export

#### Overview

The PostgreSQL Bulk Export processor provides high-performance data extraction from PostgreSQL databases using the native `COPY (query) TO STDOUT` protocol. It supports both table-based and custom query-based exports with advanced features including incremental extraction, state management, and automatic chunking for large datasets.

The processor leverages PostgreSQL's optimized COPY protocol for maximum throughput while providing flexible data transformation through NiFi's RecordReader/Writer framework. It's designed for efficient ETL pipelines, data synchronization, and analytical data extraction scenarios.

#### Key Features

- **High Performance**: Uses PostgreSQL COPY TO STDOUT for optimal data transfer
- **Incremental Extraction**: State-based incremental processing with maximum-value columns
- **Flexible Queries**: Support for both simple table exports and complex custom queries
- **Automatic Chunking**: Configurable row limits per FlowFile for memory management
- **Schema Inference**: Automatic schema detection via temporary view creation
- **State Management**: Persistent state tracking for incremental processing
- **Multiple Formats**: Output to CSV, JSON, Avro, Parquet, and other NiFi-supported formats

#### Data Flow Process

1. **Query Preparation**: Validates and prepares the export query
2. **Schema Inference**: Creates temporary view to determine result schema
3. **State Management**: Applies incremental filtering based on stored state
4. **Data Export**: Executes COPY (query) TO STDOUT for data extraction
5. **Processing**: Parses CSV stream and converts to target format
6. **Chunking**: Splits large results into multiple FlowFiles if configured
7. **State Update**: Updates maximum-value state for next incremental run

#### Configuration Properties

| Property | Required | Description |
|----------|----------|-------------|
| PostgreSQL Connection Provider | Yes | Connection pool service for PostgreSQL database |
| Table Name | No | Source table name (alternative to Custom Query) |
| Custom Query | No | SQL query for data extraction (alternative to Table Name) |
| Record Reader | Yes | Service for parsing COPY CSV output |
| Record Writer | Yes | Service for writing output records |
| Maximum-value Columns | No | Comma-separated columns for incremental processing |
| Max Rows Per Flow File | No | Maximum records per FlowFile (0 = unlimited) |
| Data Format | No | Output format: CSV (default) or Parquet |

#### CSV Configuration

The processor uses PostgreSQL's standard CSV format for COPY operations:

| Setting | Default Value | Description |
|---------|---------------|-------------|
| Delimiter | `,` (comma) | Field separator character |
| Quote Character | `"` (double quote) | Character for quoting fields |
| Escape Character | `"` (double quote) | Character for escaping quotes |
| NULL Representation | Empty string | How NULL values are represented |
| Header | Included | Column names in first row |

**Important**: Configure your RecordReader with these exact settings to ensure compatibility with PostgreSQL's COPY output format.

#### Incremental Processing

The processor supports sophisticated incremental extraction using maximum-value columns:

**Single Column Incremental:**
```sql
-- Automatically appends: WHERE updated_at > 'last_max_value'
SELECT * FROM orders WHERE status = 'completed'
```

**Multiple Column Incremental:**
```sql
-- Supports composite keys: WHERE (col1, col2) > ('val1', 'val2')
SELECT * FROM transactions WHERE amount > 1000
```

**State Storage:**
- State is stored per processor instance
- Survives NiFi restarts and cluster failovers
- Can be cleared via processor configuration

#### Performance Characteristics

- **Native COPY TO STDOUT:** Leverages PostgreSQL's optimized export protocol for maximum throughput
- **Streaming Architecture:** No intermediate buffering ensures memory efficiency
- **Automatic Chunking:** Configurable row limits prevent memory exhaustion
- **State Management:** Efficient incremental processing with persistent state tracking

#### Query Examples

**Basic Table Export:**
```sql
-- Table Name: "customers"
-- Equivalent to: SELECT * FROM customers
```

**Custom Query with Joins:**
```sql
SELECT 
    c.customer_id,
    c.name,
    c.email,
    COUNT(o.order_id) as order_count,
    MAX(o.order_date) as last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email
```

**Incremental with Time-based Column:**
```sql
-- Maximum-value Columns: "updated_at"
SELECT * FROM products 
WHERE category = 'electronics'
-- Processor automatically adds: AND updated_at > '2024-01-01 10:30:00'
```

**Incremental with Composite Key:**
```sql
-- Maximum-value Columns: "partition_date,sequence_id"
SELECT * FROM events 
WHERE event_type = 'user_action'
-- Processor adds: AND (partition_date, sequence_id) > ('2024-01-01', 12345)
```

#### Data Type Support

The processor handles all PostgreSQL data types through proper RecordReader configuration:

- **Numeric Types**: INTEGER, BIGINT, DECIMAL, NUMERIC, REAL, DOUBLE PRECISION
- **Text Types**: VARCHAR, TEXT, CHAR with proper CSV escaping
- **Date/Time Types**: DATE, TIME, TIMESTAMP, TIMESTAMPTZ (ISO format)
- **JSON Types**: JSON, JSONB as text strings
- **Binary Types**: BYTEA (hex format in CSV)
- **Array Types**: PostgreSQL array format (e.g., `{1,2,3}`)
- **Custom Types**: ENUM, UUID, and user-defined types

#### Error Handling

Comprehensive error handling ensures reliable operation:

- **Connection Failures**: Automatic retry through connection pool
- **Query Errors**: Detailed SQL error messages with context
- **Schema Mismatches**: Clear validation error messages
- **Memory Issues**: Automatic chunking for large result sets
- **State Corruption**: Graceful handling with state reset options
- **Temporary View Cleanup**: Guaranteed cleanup even on failures

#### Memory Management

The processor implements several memory optimization strategies:

- **Streaming Processing**: No intermediate result buffering
- **Configurable Chunking**: Split large results across multiple FlowFiles
- **Connection Pooling**: Efficient database connection reuse
- **Temporary View Cleanup**: Immediate cleanup to prevent schema bloat

#### Security Considerations

- **SQL Injection Protection**: Parameterized queries and identifier quoting
- **Connection Security**: SSL/TLS support via connection pool
- **Access Control**: Respects PostgreSQL user permissions and RLS
- **Audit Trail**: Complete provenance tracking in NiFi
- **Credential Management**: Secure handling through NiFi controller services

**Environment Variable Integration:**
```bash
export PGHOST=mydb.example.com
export PGUSER=app_user
export PGPASSWORD=secure_password
export PGSSLMODE=verify-full
```
Connection pool can reference these through NiFi expression language for secure credential management.

#### Monitoring and Troubleshooting

**Key Metrics:**
- `record.count`: Number of records exported
- `mime.type`: Output format type
- Processing time via NiFi provenance
- State progression for incremental jobs

**Quick Troubleshooting:**

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Slow performance | Missing indexes or complex query | Optimize query; add indexes on filter/join columns |
| Memory errors | Large result set | Reduce Max Rows Per Flow File; enable chunking |
| Schema changes fail | DDL changes during export | Retry export; ensure no concurrent DDL operations |
| State not advancing | Incremental column not updating | Verify maximum-value columns are being updated |
| SSL/TLS errors | Certificate issues | Check SSL mode and certificate paths |
| Connection timeout | Long-running query | Optimize query; increase transaction timeout |
| Empty results | WHERE clause too restrictive | Check query filters and incremental state |

**Debug Logging:** Set `org.apache.nifi.processors.postgresql` to DEBUG level in logback.xml

#### Integration Patterns

**ETL Pipeline:**
1. Export data from PostgreSQL
2. Transform using NiFi processors
3. Load into target systems
4. Monitor via NiFi provenance

**Data Lake Ingestion:**
1. Incremental export from operational databases
2. Convert to Parquet format
3. Store in distributed file systems
4. Trigger downstream analytics

**Real-time Analytics:**
1. Frequent incremental exports
2. Stream to message queues
3. Process in real-time analytics engines
4. Update dashboards and alerts

**Data Synchronization:**
1. Export changes from source systems
2. Apply transformations and validations
3. Sync to target databases
4. Maintain audit trails

#### Best Practices

1. **Query Optimization**: Use appropriate indexes for export queries
2. **Batch Sizing**: Configure Max Rows Per Flow File based on memory constraints
3. **Incremental Strategy**: Choose appropriate maximum-value columns
4. **Connection Pooling**: Size pools appropriately for concurrent exports
5. **Monitoring**: Implement alerting for failed or stalled exports
6. **Testing**: Validate export queries with representative data volumes
7. **Schema Management**: Plan for schema evolution in source tables

#### Advanced Configuration

**Custom RecordReader Configuration:**
```properties
# CSV Reader for PostgreSQL COPY format
csv.format = DEFAULT
csv.delimiter = ,
csv.quote.character = "
csv.escape.character = "
csv.comment.marker = 
csv.null.string = 
csv.header.line = true
csv.ignore.csv.header = false
```

**Performance Tuning:**
- Adjust `fetch.size` in connection pool for memory vs. latency trade-offs
- Use `Max Rows Per Flow File` to prevent memory issues with large exports
- Configure appropriate connection pool sizes for concurrent processing

#### References

- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [PostgreSQL CREATE VIEW](https://www.postgresql.org/docs/current/sql-createview.html)
- [NiFi State Management](https://nifi.apache.org/docs/nifi-docs/html/state-manager.html)
- [NiFi RecordReader/Writer Services](https://nifi.apache.org/docs/nifi-docs/html/record-path-guide.html)


