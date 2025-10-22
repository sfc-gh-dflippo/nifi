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

### PostgreSQL Bulk Load

#### Overview

The PostgreSQL Bulk Load processor provides high-performance data loading into PostgreSQL tables using the native `COPY FROM STDIN` protocol. This processor reads incoming FlowFiles using a configured `RecordReader`, converts the data to PostgreSQL-compatible CSV format, and streams it directly to the database for optimal performance.

**Key Features:**
- Native PostgreSQL COPY protocol for maximum throughput
- Support for CSV and Parquet input formats
- Automatic data type conversion and validation
- Memory-efficient streaming for large datasets
- Transaction-safe operations with proper error handling

#### Performance Characteristics

- **High Throughput:** Native COPY protocol provides optimal bulk data transfer
- **Memory Efficient:** Streaming architecture handles unlimited dataset sizes
- **Scalability:** Handles datasets of any size through streaming
- **Reliability:** Transaction-safe with automatic rollback on errors

#### Configuration

##### Required Properties

| Property | Description | Example |
|----------|-------------|---------|
| PostgreSQL Connection Provider | Reference to PostgreSQL connection service | `PostgreSQLConnectionPool` |
| Target Table | Name of the target PostgreSQL table | `users`, `${table.name}` |
| Record Reader | Service for reading input FlowFiles | `CSVReader`, `JSONReader` |
| CSV Record Writer | Service for writing CSV to PostgreSQL | `CSVRecordSetWriter` |

##### Optional Properties

| Property | Default | Description |
|----------|---------|-------------|
| Column Names | (auto-detect) | Comma-separated list of target columns |
| Batch Size | 10000 | Number of records to process per batch |
| Transaction Timeout | 30 seconds | Maximum time for transaction completion |

#### CSV Format Requirements

The processor uses PostgreSQL's default COPY CSV format:

```csv
# PostgreSQL COPY CSV Format
Delimiter: ,
Quote Character: "
Escape Character: "
NULL Representation: (empty field)
Header Line: Not included
```

**Important:** Your CSV Record Writer must be configured with these exact settings to ensure compatibility.

#### Usage Examples

##### Basic Data Loading

```xml
<!-- CSV Record Writer Configuration -->
<property name="Delimiter" value=","/>
<property name="Quote Character" value="&quot;"/>
<property name="Escape Character" value="&quot;"/>
<property name="Null String" value=""/>
<property name="Include Header Line" value="false"/>
```

##### Loading with Column Mapping

```
Target Table: users
Column Names: id,name,email,created_at
```

This will execute:
```sql
COPY users (id,name,email,created_at) FROM STDIN WITH CSV
```

##### Loading with Expression Language

```
Target Table: ${table.prefix}_${table.name}
Column Names: ${column.list}
```

#### Data Type Handling

The processor automatically handles PostgreSQL data types:

| Input Type | PostgreSQL Type | Notes |
|------------|-----------------|-------|
| String | TEXT, VARCHAR | Direct mapping |
| Integer | INTEGER, BIGINT | Automatic conversion |
| Decimal | NUMERIC, DECIMAL | Precision preserved |
| Boolean | BOOLEAN | true/false conversion |
| Date/Time | TIMESTAMP, DATE | ISO format expected |
| JSON | JSONB | Automatic JSON validation |

#### Error Handling

The processor provides comprehensive error handling:

- **Validation Errors:** Invalid data types or format issues
- **Constraint Violations:** Primary key, foreign key, or check constraints
- **Connection Errors:** Database connectivity or timeout issues
- **Transaction Errors:** Rollback on any failure to maintain consistency

**Error Relationships:**
- `success` - FlowFile processed successfully
- `failure` - Processing failed, FlowFile routed for error handling

#### Performance Tuning

##### Optimal Batch Sizes

```
Small datasets (< 10K records): Batch Size = 1000
Medium datasets (10K - 1M records): Batch Size = 10000
Large datasets (> 1M records): Batch Size = 50000
```

##### Memory Optimization

```
# For memory-constrained environments
Batch Size: 5000
Transaction Timeout: 60 seconds

# For high-performance environments  
Batch Size: 50000
Transaction Timeout: 300 seconds
```

#### Integration Patterns

##### ETL Pipeline Pattern

```
[Source] → [ConvertRecord] → [PostgreSQLBulkLoad] → [Success Handler]
                                      ↓
                                [Error Handler]
```

##### CDC Pattern with Staging

```
[CDC Source] → [RouteOnAttribute] → [PostgreSQLBulkLoad(staging)] → [PostgreSQLBulkUpsert]
```

##### Batch Processing Pattern

```
[ListFile] → [FetchFile] → [SplitRecord] → [PostgreSQLBulkLoad] → [MergeContent]
```

#### Monitoring and Troubleshooting

##### Key Metrics to Monitor

- **Throughput:** Records processed per second
- **Error Rate:** Percentage of failed FlowFiles
- **Transaction Time:** Time per batch completion
- **Connection Pool:** Active/idle connection counts

##### Quick Troubleshooting

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Slow performance | Batch size too small or network latency | Increase batch size; check network and PostgreSQL config |
| Memory errors | Dataset too large | Reduce batch size; increase JVM heap |
| Data type errors | Schema mismatch | Verify RecordReader schema matches table structure |
| Connection timeout | Pool exhausted or transaction too long | Increase Max Wait Time and Transaction Timeout |
| SSL/TLS errors | Certificate issues | Check SSL mode and certificate paths |
| Constraint violations | Data integrity issues | Validate input data; check constraints and indexes |

**Debug Logging:** Set `org.apache.nifi.processors.postgresql` to DEBUG level in logback.xml

#### Advanced Configuration

##### Custom SQL Execution

The processor supports custom column mapping and transformation:

```sql
-- Target table with computed columns
CREATE TABLE users_processed (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    processed_at TIMESTAMP DEFAULT NOW()
);
```

```
Target Table: users_processed
Column Names: name,email
-- processed_at will use DEFAULT value
```

##### Parquet Integration

When pg_parquet extension is available:

```
# Processor automatically detects Parquet support
# Provides significant compression benefits over CSV
# Optimal for analytical workloads and data lake ingestion
```

#### Security Considerations

- **Connection Security:** Use SSL/TLS for database connections
- **Credential Management:** Never hardcode database credentials
- **Data Validation:** Validate input data to prevent injection attacks
- **Access Control:** Use dedicated database users with minimal privileges

**Environment Variable Integration:**
```bash
export PGHOST=mydb.example.com
export PGUSER=app_user
export PGPASSWORD=secure_password
export PGSSLMODE=verify-full
```
Connection pool can reference these through NiFi expression language for secure credential management.

#### References

- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [NiFi Record Processing](https://nifi.apache.org/docs/nifi-docs/html/record-path-guide.html)
- [PostgreSQL Performance Tuning](https://wiki.postgresql.org/wiki/Performance_Optimization)


