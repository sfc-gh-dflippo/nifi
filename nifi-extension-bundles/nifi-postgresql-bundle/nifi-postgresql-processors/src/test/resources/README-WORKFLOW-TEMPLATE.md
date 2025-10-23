# PostgreSQL Bulk Operations Workflow Template

This NiFi flow template demonstrates a complete workflow using the PostgreSQL bulk operation processors.

## Workflow Overview

The template implements the following workflow:

1. **Create Source Table** - Creates the source table if it doesn't exist
2. **Create Target Table** - Creates the target table for storing results
3. **Generate Test Data** - Generates random employee data in CSV format (10 sample records)
4. **Bulk Load to Source** - Uses `PostgreSQLBulkLoad` to load data into the source table
5. **Bulk Export** - Uses `PostgreSQLBulkExport` to export data from the source table
6. **Update Data** - Increases all salaries by 10%
7. **Bulk Upsert** - Uses `PostgreSQLBulkUpsert` to update records in the source table
8. **Bulk Load to Target** - Uses `PostgreSQLBulkLoad` to load upsert results into the target table
9. **Log Success** - Logs successful completion

## Table Schema

Both tables use the following schema:

```sql
CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(100),
  age INTEGER,
  salary DECIMAL(10,2),
  department VARCHAR(50),
  hire_date DATE,
  is_active BOOLEAN,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Parameters

The workflow uses the following parameterized values at the process group level:

### Table Parameters
- **source.table.name** - Name of the source table (default: `employees`)
- **target.table.name** - Name of the target table (default: `employees_updated`)

### PostgreSQL Connection Parameters
- **pg.host** - PostgreSQL server hostname (default: `localhost`)
- **pg.port** - PostgreSQL server port (default: `5432`)
- **pg.database** - Database name (default: `nifi_test`)
- **pg.username** - PostgreSQL username (default: `nifi_user`)
- **pg.password** - PostgreSQL password (sensitive, no default)
- **pg.connection.pool** - Reference to the PostgreSQL Connection Pool service

## Prerequisites

### 1. PostgreSQL Connection Pool Controller Service

You need to configure a `PostgreSQLConnectionPool` controller service with:

**Service ID**: Match the `pg.connection.pool` parameter value

**Configuration**:
```
Host: #{pg.host}
Port: #{pg.port}
Database: #{pg.database}
Username: #{pg.username}
Password: #{pg.password}
```

Or simply use the connection URL:
```
jdbc:postgresql://#{pg.host}:#{pg.port}/#{pg.database}
```

### 2. PostgreSQL Database

Ensure you have:
- A running PostgreSQL instance
- A database created (or permissions to create one)
- A user with CREATE TABLE and data manipulation permissions

## How to Import and Use

### Step 1: Import the Template

1. In NiFi UI, click the **Templates** icon (document with arrow)
2. Click **Browse** 
3. Select `PostgreSQL-Bulk-Operations-Workflow.json`
4. Click **Upload**

### Step 2: Add Template to Canvas

1. Drag the **Template** icon from the toolbar to the canvas
2. Select "PostgreSQL Bulk Operations Workflow"
3. Click **Add**

### Step 3: Configure Parameters

1. Right-click on the process group
2. Select **Variables** or **Parameters**
3. Set your PostgreSQL connection details:
   - `pg.host`: Your PostgreSQL server hostname
   - `pg.port`: Your PostgreSQL port (usually 5432)
   - `pg.database`: Your database name
   - `pg.username`: Your username
   - `pg.password`: Your password (will be encrypted)
   - `source.table.name`: Name for source table (e.g., "employees")
   - `target.table.name`: Name for target table (e.g., "employees_archive")

### Step 4: Configure Controller Service

1. Right-click on the canvas → **Controller Services**
2. Add **PostgreSQLConnectionPool** service
3. Configure with your connection parameters (can reference the parameters you set above)
4. Enable the service

### Step 5: Run the Workflow

1. Right-click the process group → **Start**
2. The workflow will:
   - Create tables if they don't exist
   - Generate sample data
   - Load it into PostgreSQL
   - Export and transform it
   - Upsert back to the source table
   - Load results into the target table

## Expected Results

After successful execution:

1. **Source Table** (`employees`): Contains 10 employee records with updated salaries (+10%)
2. **Target Table** (`employees_updated`): Contains the same 10 records from the upsert operation
3. **FlowFiles**: Successfully processed through all stages

## Monitoring and Troubleshooting

### Check Processor Status
- Green checkmark: Processor completed successfully
- Red warning: Check bulletin board for errors

### Common Issues

**Connection Refused**
- Verify PostgreSQL is running
- Check host/port parameters
- Verify firewall settings

**Authentication Failed**
- Verify username/password
- Check PostgreSQL `pg_hba.conf` for access rules

**Table Already Exists**
- The CREATE TABLE uses `IF NOT EXISTS` so this should not be an error
- If you see issues, manually drop the tables and re-run

**Permission Denied**
- User needs CREATE TABLE permission
- User needs INSERT, UPDATE, SELECT permissions on the schema

## Testing with Different Data

To test with your own data:

1. **Modify the "Generate Random Test Data" processor**:
   - Replace the CSV data in the ReplaceText processor
   - Ensure headers match the table schema

2. **Adjust the "Update Salary Field" processor**:
   - Modify the regex to transform different fields
   - Change the calculation (currently multiplies by 1.1 for +10%)

## Customization

### Change Batch Sizes
All bulk processors have a `Batch Size` property (default: 1000)
- Increase for better performance with large datasets
- Decrease if experiencing memory issues

### Modify Table Schema
1. Update the CREATE TABLE statements in ExecuteSQL processors
2. Update the CSV headers in the Generate Test Data processor
3. Adjust the Update processor regex pattern

### Add Error Handling
Consider adding processors to handle failure relationships:
- LogAttribute for debugging
- PutFile to save failed records
- RouteOnAttribute for conditional processing

## Performance Tips

1. **Batch Size**: Set appropriately for your data volume
2. **Connection Pool**: Configure max connections based on concurrency
3. **Concurrent Tasks**: Adjust processor concurrency for parallel processing
4. **CSV vs Parquet**: For large datasets, consider using Parquet format

## Related Documentation

- [PostgreSQLBulkLoad Processor](../docs/org.apache.nifi.processors.postgresql.PostgreSQLBulkLoad/additionalDetails.md)
- [PostgreSQLBulkExport Processor](../docs/org.apache.nifi.processors.postgresql.PostgreSQLBulkExport/additionalDetails.md)
- [PostgreSQLBulkUpsert Processor](../docs/org.apache.nifi.processors.postgresql.PostgreSQLBulkUpsert/additionalDetails.md)

## Example Use Cases

This workflow pattern is useful for:

1. **Data Synchronization**: Keep tables in sync with periodic updates
2. **ETL Pipelines**: Extract, transform, and load data between tables
3. **Data Auditing**: Archive original data before updates
4. **Testing**: Validate bulk operation performance and correctness
5. **Migration**: Move data between environments with transformations

## Support

For issues or questions:
- Check the processor documentation
- Review NiFi logs for detailed error messages
- Consult the PostgreSQL bundle README.md

