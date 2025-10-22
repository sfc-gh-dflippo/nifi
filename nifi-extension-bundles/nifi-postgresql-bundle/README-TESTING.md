# PostgreSQL Bundle Testing Guide

## Overview

This guide covers test database setup, credential management, and running tests for the PostgreSQL bundle.

## Security Note

**🚨 IMPORTANT**: Database credentials are **never** hardcoded in source code. Tests use the `CredentialManager` class which implements a three-tier credential resolution system:

1. **Configuration files** (recommended for local development)
   - PostgreSQL: `~/.pg_service.conf`
   - Snowflake: `~/.snowflake/connections.toml`
2. **Environment variables** (fallback)
3. **Default values** (localhost test mode)

## Credential Management

### PostgreSQL Credentials

The `CredentialManager` loads PostgreSQL credentials in priority order:

#### Option 1: ~/.pg_service.conf (Recommended)

Create or edit `~/.pg_service.conf`:

```ini
[default]
host=your-database-host.com
port=5432
dbname=your_database
user=your_username
password=your_password
sslmode=require
```

**Security:** Ensure proper file permissions:
```bash
chmod 600 ~/.pg_service.conf
```

#### Option 2: Environment Variables

```bash
export PGHOST=your-database-host.com
export PGPORT=5432
export PGDATABASE=your_database
export PGUSER=your_username
export PGPASSWORD=your_password
export PGSSLMODE=require
```

#### Option 3: Default Localhost (Automatic)

If no configuration is found, tests automatically use:
- Host: `localhost`
- Port: `5432`
- Database: `postgres`
- User: `postgres`
- Password: (empty)
- SSL Mode: `prefer`

### Snowflake Credentials (Optional)

If you're working with Snowflake integration tests, create `~/.snowflake/connections.toml`:

```toml
[default]
account = "your-account"
user = "your-username"
password = "your-password"
role = "your-role"
warehouse = "your-warehouse"
database = "your-database"
schema = "your-schema"

# For key pair authentication
# private_key_file = "/path/to/rsa_key.p8"
# private_key_file_pwd = "your-key-password"
```

**Security:** Ensure proper file permissions:
```bash
chmod 600 ~/.snowflake/connections.toml
```

## Test Database Setup

### Option 1: Docker (Recommended)

```bash
# Start PostgreSQL container with pg_parquet support
docker run --name nifi-postgres-test \
  -e POSTGRES_DB=nifi_test \
  -e POSTGRES_USER=nifi_test \
  -e POSTGRES_PASSWORD=nifi_test \
  -p 5432:5432 \
  -d postgres:13

# Verify container is running
docker ps | grep nifi-postgres-test

# Test connection
docker exec nifi-postgres-test psql -U nifi_test -d nifi_test -c "SELECT version();"
```

### Option 2: Local PostgreSQL

```sql
-- Connect as superuser and create test database and user
CREATE DATABASE nifi_test;
CREATE USER nifi_test WITH PASSWORD 'nifi_test';
GRANT ALL PRIVILEGES ON DATABASE nifi_test TO nifi_test;

-- Connect to nifi_test database
\c nifi_test

-- Create schema and grant permissions
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA public TO nifi_test;
GRANT ALL ON ALL TABLES IN SCHEMA public TO nifi_test;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO nifi_test;
```

### Connecting Tests to Your Database

After setting up the test database, configure your credentials using one of the methods in the [Credential Management](#credential-management) section above. For Docker setup, you can use:

```bash
# Add to ~/.pg_service.conf
[default]
host=localhost
port=5432
dbname=nifi_test
user=nifi_test
password=nifi_test
sslmode=prefer
```

Or use environment variables:
```bash
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=nifi_test
export PGUSER=nifi_test
export PGPASSWORD=nifi_test
```

## Running Tests

### All Tests
```bash
mvn clean test
```

### Unit Tests Only
```bash
mvn test -Dtest=*Test
```

### Integration Tests Only
```bash
mvn test -Dtest=*IT
```

### Specific Test Classes
```bash
# Processor integration tests
mvn test -Dtest=PostgreSQLProcessorsIT

# Parquet format tests
mvn test -Dtest=ParquetProcessorsIT

# Error handling tests
mvn test -Dtest=ErrorProcessorsIT

# Credential management tests
mvn test -Dtest=CredentialManagerTest

# Utility tests
mvn test -Dtest=MaxValueTrackerTest
```

### Skip Integration Tests
```bash
mvn test -DskipITs=true
```

## Test Structure

```
src/test/java/
├── integration/
│   ├── PostgreSQLProcessorsIT.java         # Main processor functionality
│   ├── PerformanceTestIT.java              # Performance benchmarks
│   ├── ETLPerformanceTestIT.java           # End-to-end ETL validation
│   ├── credentials/
│   │   ├── CredentialManager.java          # Test credential management
│   │   └── CredentialManagerTest.java      # Credential resolution tests
│   └── sql/
│       └── PostgreSQLUpsertTemplates.java  # SQL template utilities
├── format/
│   └── ParquetProcessorsIT.java            # Parquet format tests
├── error/
│   └── ErrorProcessorsIT.java              # Error handling tests
├── util/
│   └── MaxValueTrackerTest.java            # Utility unit tests
├── PostgreSQLBulkLoadTest.java             # Load processor unit tests
├── PostgreSQLBulkExportTest.java           # Export processor unit tests
├── PostgreSQLBulkUpsertTest.java           # Upsert processor unit tests
└── PostgreSQLConfigAware.java              # Test configuration interface
```

## Troubleshooting Tests

### Database Connection Failures

```bash
# Verify PostgreSQL is running
pg_isready -h localhost -p 5432

# Test connection with credentials
psql -h localhost -p 5432 -U nifi_test -d nifi_test -c "SELECT version();"

# Check if database exists
psql -h localhost -p 5432 -U postgres -l | grep nifi_test
```

### Permission Errors

```sql
-- Ensure test user has all necessary permissions
GRANT ALL PRIVILEGES ON DATABASE nifi_test TO nifi_test;
GRANT ALL ON SCHEMA public TO nifi_test;
GRANT ALL ON ALL TABLES IN SCHEMA public TO nifi_test;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO nifi_test;
```

### Port Conflicts

```bash
# Check if port 5432 is already in use
lsof -i :5432
# or
netstat -an | grep 5432

# Use different port for test database
docker run -p 5433:5432 ...
export POSTGRESQL_TEST_PORT=5433
```

### Test Failures with Docker

```bash
# Check container logs
docker logs nifi-postgres-test

# Restart container
docker restart nifi-postgres-test

# Remove and recreate container
docker rm -f nifi-postgres-test
docker run --name nifi-postgres-test ...
```

### Clean Test Database

```sql
-- Drop all tables in test database
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO nifi_test;
```

## Performance Testing

### Running Benchmarks

```bash
# Run performance tests
mvn test -Dtest=PerformanceTestIT

# Run ETL pipeline tests
mvn test -Dtest=ETLPerformanceTestIT
```

### Benchmark Configuration

Performance tests use larger datasets and longer timeouts. Ensure adequate resources:
- Minimum 4GB available memory
- SSD storage for PostgreSQL data
- Clean database state before running

## Test Data

Test resources are located in `src/test/resources/`:

```
src/test/resources/
├── csv/
│   ├── simple.csv                    # Small dataset for basic tests
│   ├── test_data.csv                 # Standard test dataset
│   └── test_data_no_header.csv       # CSV without headers
├── parquet/
│   └── simple.parquet                # Parquet format test data
├── keys/
│   ├── test-client.crt               # Test SSL certificate
│   ├── test-client.key               # Test SSL key
│   └── test-ca.crt                   # Test CA certificate
├── docker/
│   └── postgresql-pgparquet/
│       ├── Dockerfile                # Custom PostgreSQL with pg_parquet
│       └── init-pg-parquet.sql       # Extension setup script
└── test-config.properties.template   # Configuration template
```

## Security Best Practices

### Credential Files
- **Never commit credential files** to version control
- Set proper file permissions: `chmod 600 ~/.pg_service.conf` and `chmod 600 ~/.snowflake/connections.toml`
- Use separate test credentials from production
- Rotate credentials periodically

### Development
- Use `~/.pg_service.conf` for local development (most convenient)
- Use Docker for isolated test environments
- Use environment variables for automation/CI

### CredentialManager Features
- **Zero hardcoded credentials** in source code
- **Three-tier resolution** with automatic fallback
- **Type-safe credential structures** with validation
- **Encrypted private key support** for Snowflake (Bouncy Castle)
- **Comprehensive error handling** with clear messages

## Additional Notes

- Tests automatically skip if database is unavailable (fails gracefully)
- Integration tests require active PostgreSQL connection
- Unit tests run without database dependencies
- Test database is not modified between test runs (tests clean up after themselves)
- Some tests may be skipped if optional dependencies (pg_parquet) are not available
