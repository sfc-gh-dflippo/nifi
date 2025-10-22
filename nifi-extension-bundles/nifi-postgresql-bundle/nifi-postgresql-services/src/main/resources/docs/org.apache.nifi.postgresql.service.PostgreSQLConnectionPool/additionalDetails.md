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

### PostgreSQL Connection Pool

#### Overview

The PostgreSQL Connection Pool provides enterprise-grade database connection pooling built on Apache DBCP2. It serves as the central connection management service for all PostgreSQL processors, offering flexible configuration options, comprehensive SSL/TLS support, and optimized performance tuning capabilities.

This service supports both simple full-URL configuration and component-based configuration with explicit host/port/database parameters, making it suitable for various deployment scenarios from development to production.

#### Key Features

- **Flexible Configuration**: Full JDBC URL or component-based (host/port/database) configuration
- **Connection Pooling**: Apache DBCP2-based pooling with configurable pool sizes and lifecycle management
- **SSL/TLS Security**: Comprehensive encryption support including client certificates and various SSL modes
- **Performance Optimization**: Prepared statement caching, rewrite batched inserts, and adaptive fetch modes
- **Expression Language**: Most properties support NiFi Expression Language for dynamic configuration
- **Kerberos/GSSAPI**: Enterprise authentication integration support
- **CopyManager Integration**: Native PostgreSQL COPY protocol support for bulk operations
- **Dynamic Properties**: Support for any PostgreSQL JDBC driver property

#### Configuration Modes

The service supports two primary configuration approaches:

##### Option 1: Full JDBC URL

Provides complete control over the connection URL with all parameters embedded:

```
Connection URL Format: Full URL
Database Connection URL: jdbc:postgresql://myhost:5432/mydb?sslmode=require&ApplicationName=NiFi
Database User: app_user
Password: app_password
```

**Use this when:**
- Migrating from existing JDBC configurations
- Need full control over JDBC URL parameters
- Working with complex connection strings

##### Option 2: Component-Based Configuration

Constructs the JDBC URL from individual components:

```
Connection URL Format: Host Name
Database Host: myhost.example.com
Database Port: 5432
Database Name: production_db
Database Schema: public
Database User: app_user
Password: app_password
SSL Mode: require
```

**Use this when:**
- Environment-specific configurations (dev/test/prod)
- Using Expression Language for dynamic values
- Clear separation of connection parameters

#### SSL/TLS Configuration

The service provides comprehensive SSL/TLS support for secure database connections:

##### SSL Mode Options

| Mode | Description | Use Case |
|------|-------------|----------|
| `disable` | No SSL encryption | Local development only |
| `allow` | SSL if server supports it | Non-critical environments |
| `prefer` | Prefer SSL, fallback to plain | Transitional environments |
| `require` | Require SSL, no certificate validation | Basic encryption needs |
| `verify-ca` | Require SSL with CA validation | Production environments |
| `verify-full` | Require SSL with full validation | High-security production |

##### Client Certificate Authentication

For mutual TLS (mTLS) authentication:

```
SSL Mode: verify-full
SSL Certificate: /etc/nifi/certs/client.crt
SSL Key: /etc/nifi/certs/client.key
SSL Root Certificate: /etc/nifi/certs/ca.crt
SSL Password: ${ssl.keystore.password}
```

**Certificate Requirements:**
- Client certificate must be in PEM format
- Private key can be encrypted or unencrypted
- Root certificate validates the server's certificate chain

##### SSL Hostname Verification

```
SSL Hostname Verifier: 
  - (default): Standard hostname verification
  - org.postgresql.ssl.NonValidatingFactory: Disables verification
  - Custom implementation: For non-standard verification
```

#### Connection Pool Settings

##### Basic Pool Configuration

| Property | Default | Description |
|----------|---------|-------------|
| Max Total Connections | 8 | Maximum active connections in the pool |
| Max Idle Connections | 8 | Maximum idle connections to maintain |
| Min Idle Connections | 0 | Minimum idle connections to maintain |
| Max Wait Time | 500 millis | Maximum time to wait for available connection |
| Validation Query | SELECT 1 | Query to validate connections |

##### Advanced Pool Configuration

| Property | Default | Description |
|----------|---------|-------------|
| Max Connection Lifetime | -1 (infinite) | Maximum lifetime of a connection |
| Eviction Run Period | -1 (disabled) | Interval for idle connection eviction |
| Min Evictable Idle Time | 30 min | Minimum idle time before eviction |
| Soft Min Evictable Idle Time | -1 (infinite) | Soft minimum idle time with min idle |

##### Pool Sizing Guidelines

**Low-Concurrency Workloads** (1-3 concurrent processors):
```
Max Total Connections: 5
Max Idle Connections: 3
Min Idle Connections: 1
```

**Medium-Concurrency Workloads** (4-8 concurrent processors):
```
Max Total Connections: 10
Max Idle Connections: 8
Min Idle Connections: 2
```

**High-Concurrency Workloads** (8+ concurrent processors):
```
Max Total Connections: 20
Max Idle Connections: 10
Min Idle Connections: 5
```

**Formula**: `Max Total Connections >= (Concurrent Processors × 2) + Buffer`

#### Performance Tuning

##### Prepared Statement Caching

Significantly improves performance for repeated queries:

```
Prepared Statement Cache Queries: 256
Prepared Statement Cache Size (MiB): 5
Prepared Statement Cache SQL Limit: 2048
```

**Benefits:**
- Reduces query parsing overhead
- Improves execution plan reuse
- Optimal for repetitive operations

##### Batch Operations

Enable batch rewriting for bulk operations:

```
Rewrite Batched Inserts: true
```

**Impact:** Transforms multiple INSERT statements into single multi-value INSERT for better performance.

##### Fetch Size Optimization

Controls memory vs. network trade-offs:

```
Default Row Fetch Size: 10000
Adaptive Fetch: false
Max Result Buffer: 0 (unlimited)
```

**Guidelines:**
- **Large datasets**: Increase fetch size (50000+) for better throughput
- **Memory constrained**: Decrease fetch size (1000-5000)
- **Adaptive fetch**: Enable for variable result set sizes

##### Query Mode Selection

```
Preferred Query Mode:
  - simple: Best for single-use queries
  - extended: Better for prepared statements (default)
  - extendedForPrepared: Extended only for prepared, simple otherwise
  - extendedCacheEverything: Aggressive caching
```

##### Connection Timeouts

```
Login Timeout: 30 seconds
Connect Timeout: 10 seconds
Socket Timeout: 0 (infinite)
Cancel Signal Timeout: 10 seconds
```

**Recommendations:**
- Increase timeouts for high-latency networks
- Use socket timeout to prevent hung connections
- Balance between reliability and responsiveness

#### Kerberos and GSSAPI Authentication

For enterprise Kerberos integration:

```
Kerberos Server Name: postgres/dbserver.example.com
JAAS Application Name: Client
GSS Encryption Mode: prefer
```

**Prerequisites:**
- Valid Kerberos principal and keytab
- Properly configured JAAS configuration
- Network connectivity to KDC

#### Environment Variable Integration

The service supports environment variables for secure credential management:

```bash
export PGHOST=mydb.example.com
export PGPORT=5432
export PGDATABASE=production_db
export PGUSER=app_user
export PGPASSWORD=secure_password
export PGSSLMODE=verify-full
export PGSSLCERT=/etc/nifi/certs/client.crt
export PGSSLKEY=/etc/nifi/certs/client.key
export PGSSLROOTCERT=/etc/nifi/certs/ca.crt
```

**Usage in NiFi:**
- Reference via Expression Language: `${PGHOST}`, `${PGUSER}`, etc.
- Automatic fallback for standard PostgreSQL environment variables
- Secure credential handling without hardcoding

#### Dynamic Properties

The service supports any PostgreSQL JDBC driver property as a dynamic property:

**Example Dynamic Properties:**
```
Property Name: assumeMinServerVersion
Value: 12.0

Property Name: logServerErrorDetail
Value: false

Property Name: stringtype
Value: unspecified
```

**Common Use Cases:**
- Version-specific optimizations
- Debug logging configuration
- Custom type handling

#### Usage Examples

##### Basic Development Configuration

```
Connection URL Format: Host Name
Database Host: localhost
Database Port: 5432
Database Name: dev_db
Database User: dev_user
Password: dev_password
SSL Mode: disable
Max Total Connections: 5
```

##### Production Configuration with SSL

```
Connection URL Format: Host Name
Database Host: ${PGHOST}
Database Port: ${PGPORT}
Database Name: ${PGDATABASE}
Database User: ${PGUSER}
Password: ${PGPASSWORD}
SSL Mode: verify-full
SSL Root Certificate: ${PGSSLROOTCERT}
Max Total Connections: 20
Max Idle Connections: 10
Prepared Statement Cache Queries: 256
Rewrite Batched Inserts: true
```

##### High-Performance Analytics Configuration

```
Connection URL Format: Full URL
Database Connection URL: jdbc:postgresql://analytics-db:5432/warehouse?sslmode=require&ApplicationName=NiFi-Analytics
Database User: analytics_user
Password: ${ANALYTICS_PASSWORD}
Max Total Connections: 50
Default Row Fetch Size: 50000
Prepared Statement Cache Size (MiB): 10
Rewrite Batched Inserts: true
Target Server Type: primary
```

#### Monitoring and Health Checks

##### Connection Pool Metrics

Monitor these key indicators:

- **Active Connections**: Current connections in use
- **Idle Connections**: Available connections in pool
- **Wait Time**: Time spent waiting for connections
- **Validation Failures**: Failed connection validations

##### Validation Query

The validation query ensures connection health:

```
Validation Query: SELECT 1
```

**Alternatives:**
- `SELECT version()`: Validates with server version
- `/* ping */ SELECT 1`: Application-tagged validation
- Empty (PostgreSQL driver default): Uses isValid() method

##### Health Check Best Practices

1. **Regular Validation**: Enable with appropriate eviction settings
2. **Connection Lifetime**: Set Max Connection Lifetime for long-running pools
3. **Monitoring**: Track pool metrics via NiFi monitoring
4. **Alert Thresholds**: Set alerts for pool exhaustion scenarios

#### Security Considerations

##### Credential Management

- **Never hardcode credentials** in processor configurations
- **Use environment variables** for production deployments
- **Leverage NiFi Parameter Contexts** for secure credential storage
- **Rotate credentials regularly** using Expression Language references
- **Encrypt sensitive dynamic properties** when possible

##### Access Control

- **Dedicated database users** with minimal required privileges
- **Row-level security (RLS)** policies on PostgreSQL side
- **Audit logging** for connection and query activity
- **Network segregation** between NiFi and database tiers

##### SSL/TLS Best Practices

1. Always use `verify-full` SSL mode in production
2. Store certificates in secure, access-controlled locations
3. Use encrypted private keys when possible
4. Regularly rotate SSL certificates before expiration
5. Monitor certificate expiration dates

#### Troubleshooting

##### Quick Troubleshooting

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Connection refused | Database not running or firewall blocking | Verify PostgreSQL is running; check pg_hba.conf and firewall rules |
| Authentication failed | Incorrect credentials or auth method | Verify username/password; check pg_hba.conf authentication method |
| SSL handshake failed | Certificate issues or SSL mode mismatch | Verify certificates; try different SSL modes (require → verify-ca → verify-full) |
| Connection timeout | Network latency or database overload | Increase timeouts; check network connectivity; verify database load |
| Pool exhausted | Too many concurrent operations | Increase Max Total Connections; check for connection leaks |
| Validation failures | Stale connections or network issues | Check validation query; adjust eviction settings |
| Certificate errors | Invalid or expired certificates | Verify certificate validity: `openssl x509 -in cert.crt -text -noout` |

##### Debug Logging

Enable detailed logging for troubleshooting:

**NiFi logback.xml:**
```xml
<logger name="org.apache.nifi.postgresql.service" level="DEBUG"/>
<logger name="org.postgresql" level="DEBUG"/>
<logger name="org.apache.commons.dbcp2" level="INFO"/>
```

**PostgreSQL JDBC Driver Logging:**
Add dynamic property:
```
Property Name: loggerLevel
Value: TRACE
```

##### Common Issues

**Issue: Connection pool exhausted**
```
Symptoms: "Timeout waiting for idle object" errors
Solution: 
  1. Increase Max Total Connections
  2. Reduce Max Wait Time to fail faster
  3. Check for connection leaks in custom processors
  4. Monitor active vs idle connection ratio
```

**Issue: SSL certificate validation errors**
```
Symptoms: "unable to find valid certification path" errors
Solution:
  1. Verify SSL Root Certificate path is correct
  2. Check certificate is in PEM format
  3. Ensure certificate chain is complete
  4. Try SSL Mode: require (bypasses validation) temporarily
```

**Issue: Kerberos authentication failures**
```
Symptoms: "GSS Authentication failed" errors
Solution:
  1. Verify Kerberos principal and keytab are valid
  2. Check JAAS configuration is correct
  3. Ensure KDC is reachable
  4. Test with kinit manually first
```

#### Integration with PostgreSQL Processors

This connection pool service is designed to work with the PostgreSQL processor bundle:

- **PostgreSQLBulkLoad**: High-performance data loading using COPY FROM STDIN
- **PostgreSQLBulkExport**: Efficient data export using COPY TO STDOUT
- **PostgreSQLBulkUpsert**: Advanced UPSERT operations with JSONB support

All processors share connection pooling for optimal resource utilization.

#### Best Practices

1. **Use Component-Based Configuration** in production for flexibility and Expression Language support
2. **Enable SSL/TLS** with verify-full mode for all production environments
3. **Size connection pools appropriately** based on concurrent processor instances
4. **Enable prepared statement caching** for better performance with repeated queries
5. **Set reasonable timeouts** to prevent hung connections and resource exhaustion
6. **Monitor pool metrics** regularly to identify bottlenecks or misconfigurations
7. **Use environment variables** for credentials and sensitive configuration
8. **Enable connection validation** with appropriate eviction policies
9. **Test thoroughly** in non-production environments before deploying
10. **Document custom configurations** for maintenance and troubleshooting

#### References

- [PostgreSQL JDBC Driver Documentation](https://jdbc.postgresql.org/documentation/)
- [PostgreSQL Connection Parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS)
- [Apache DBCP2 Documentation](https://commons.apache.org/proper/commons-dbcp/configuration.html)
- [PostgreSQL SSL Configuration](https://www.postgresql.org/docs/current/ssl-tcp.html)
- [NiFi Expression Language Guide](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html)



