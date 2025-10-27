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

package org.apache.nifi.processors.postgresql;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.postgresql.util.TableMetadata;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.postgresql.stream.CsvExportStreamer;
import org.apache.nifi.processors.postgresql.stream.CsvImportStreamer;
import org.apache.nifi.processors.postgresql.stream.ParquetExportStreamer;
import org.apache.nifi.processors.postgresql.util.CopyStreamUtil;
import org.apache.nifi.processors.postgresql.util.ProcessorProperties;
import org.apache.nifi.processors.postgresql.util.SqlBuilder;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

/**
 * Performs a bulk UPSERT into a target PostgreSQL table by loading a temporary table using COPY FROM STDIN (CSV or Parquet) and executing a
 * caller-provided UPSERT SQL template. Optionally streams results back using COPY (query) TO STDOUT.
 */
@Tags({"postgresql", "copy from", "bulk", "upsert", "merge"})
@CapabilityDescription("Performs bulk upsert to a target table using a temporary table loaded with COPY FROM STDIN in CSV or Parquet format. "
        + "Automatically generates INSERT ON CONFLICT DO UPDATE statement by querying table metadata for primary key and columns. "
        + "JSONB/JSON columns are merged using PostgreSQL's concatenation operator (||). "
        + "In CSV mode, a RecordReader parses incoming FlowFiles and Apache Commons CSV generates the COPY stream. "
        + "In Parquet mode, FlowFile content must be Parquet bytes and is streamed via pg_parquet. "
        + "Optionally exports results using COPY (query) TO STDOUT with JSONB columns cast to TEXT for Parquet compatibility. "
        + "REQUIRES: Target table must have a primary key.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
@WritesAttributes({@WritesAttribute(attribute = "record.count", description = "Number of input records processed for upsert"),
        @WritesAttribute(attribute = "mime.type", description = "MIME type when exporting results")})
public class PostgreSQLBulkUpsert extends AbstractProcessor {
    // Common
    static final PropertyDescriptor CONNECTION_PROVIDER = ProcessorProperties.CONNECTION_PROVIDER;
    static final PropertyDescriptor TARGET_TABLE = ProcessorProperties.TARGET_TABLE;
    static final PropertyDescriptor BULK_TRANSFER_DATA_FORMAT = ProcessorProperties.BULK_TRANSFER_DATA_FORMAT;
    static final PropertyDescriptor STREAM_INCOMING_FILE = ProcessorProperties.STREAM_INCOMING_FILE;

    // Upsert
    static final PropertyDescriptor UPSERT_RETURNS_RECORDS = ProcessorProperties.UPSERT_RETURNS_RECORDS;
    static final PropertyDescriptor ENABLE_DEDUPLICATION = ProcessorProperties.ENABLE_DEDUPLICATION;
    static final PropertyDescriptor DEDUPLICATION_TIMESTAMP_COLUMN = ProcessorProperties.DEDUPLICATION_TIMESTAMP_COLUMN;

    // CSV
    static final PropertyDescriptor RECORD_READER = ProcessorProperties.RECORD_READER;
    static final PropertyDescriptor RECORD_WRITER = ProcessorProperties.RECORD_WRITER;
    static final PropertyDescriptor CSV_TARGET_COLUMNS = ProcessorProperties.CSV_TARGET_COLUMNS;
    static final PropertyDescriptor CSV_DELIMITER = ProcessorProperties.CSV_DELIMITER;
    static final PropertyDescriptor CSV_QUOTE = ProcessorProperties.CSV_QUOTE;
    static final PropertyDescriptor CSV_ESCAPE = ProcessorProperties.CSV_ESCAPE;
    static final PropertyDescriptor CSV_NULL = ProcessorProperties.CSV_NULL;
    static final PropertyDescriptor CSV_HEADER = ProcessorProperties.CSV_HEADER;

    // Parquet (COPY FROM) option
    static final PropertyDescriptor PARQUET_MATCH_BY = ProcessorProperties.PARQUET_MATCH_BY;

    // Parquet (COPY TO) options for result export
    static final PropertyDescriptor PARQUET_VERSION = ProcessorProperties.PARQUET_VERSION;

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles successfully processed and, if configured, containing exported results").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles that failed during upsert").build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(CONNECTION_PROVIDER, TARGET_TABLE, BULK_TRANSFER_DATA_FORMAT,
            STREAM_INCOMING_FILE, UPSERT_RETURNS_RECORDS, ENABLE_DEDUPLICATION, DEDUPLICATION_TIMESTAMP_COLUMN, RECORD_READER, RECORD_WRITER, 
            CSV_TARGET_COLUMNS, CSV_DELIMITER, CSV_QUOTE, CSV_ESCAPE, CSV_NULL, CSV_HEADER, PARQUET_MATCH_BY, PARQUET_VERSION);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorProperties properties = new ProcessorProperties(context, session);
        final SqlBuilder sqlBuilder = new SqlBuilder(properties);
        FlowFile flowFile = sqlBuilder.getWorkingFlowFile();
        if (sqlBuilder.isPlaceholder()) {
            sqlBuilder.cleanup();
            return;
        }

        final String targetTable = properties.getTargetTable();
        final boolean upsertReturnsRecords = properties.getUpsertReturnsRecords();

        final Integer maxRowsPerFlowFileObj = properties.getMaxRowsPerFlowFile();
        final int maxRowsPerFlowFile = maxRowsPerFlowFileObj == null ? 0 : maxRowsPerFlowFileObj.intValue();
        final String format = properties.getBulkTransferDataFormat();

        // Parse schema and table name
        final String[] parts = targetTable.split("\\.");
        final String schema = parts.length > 1 ? parts[0] : "public";
        final String table = parts.length > 1 ? parts[1] : parts[0];

        final PostgreSQLConnectionProviderService connectionProvider = context.getProperty(CONNECTION_PROVIDER)
                .asControllerService(PostgreSQLConnectionProviderService.class);

        Connection connection = null;
        boolean originalAutoCommit = false;
        try (PostgreSQLConnectionWrapper wrapper = connectionProvider.getPostgreSQLConnection()) {
            connection = wrapper.getConnection();
            final PGConnection pgConnection = wrapper.unwrap();
            final CopyManager copyManager = pgConnection.getCopyAPI();

            // Save original autoCommit state and set to false for transaction control
            originalAutoCommit = connection.getAutoCommit();
            try {
                if (originalAutoCommit) {
                    connection.setAutoCommit(false);
                }
            } catch (SQLException e) {
                throw new ProcessException("Failed to disable auto-commit", e);
            }

            final String tempTable = "temp_nifi_" + UUID.randomUUID().toString().replaceAll("-", "");

            // Fetch table metadata from cache to validate primary key and generate upsert
            final TableMetadata metadata = connectionProvider.getTableMetadata(schema, table);
            if (!metadata.hasPrimaryKey()) {
                throw new ProcessException("Target table " + targetTable + " must have a primary key for upsert operations");
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE TEMP TABLE " + quoteIdentifier(tempTable) + " (LIKE " + quoteIdentifierPath(targetTable) + " INCLUDING ALL)");
            }

            final String copyInSql;
            final boolean streamIncoming = properties.isStreamIncomingFile();
            final RecordReaderFactory readerFactory = streamIncoming
                    ? null
                    : context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            if ("Parquet".equalsIgnoreCase(format)) {
                copyInSql = sqlBuilder.buildCopyIntoTableSql(tempTable, "");
            } else {
                final String targetColumnsCsv = getTargetColumnsCsv(properties);
                copyInSql = sqlBuilder.buildCopyIntoTableSql(tempTable, targetColumnsCsv);
            }

            // Execute COPY IN operation using true streaming approach (following PostgreSQL
            // JDBC documentation)
            final long inputRecordCount;
            if ("Parquet".equalsIgnoreCase(format)) {
                if (streamIncoming) {
                    // For streaming Parquet files, stream directly from FlowFile to PostgreSQL
                    final long[] result = new long[1];
                    session.read(flowFile, inputStream -> {
                        try {
                            result[0] = CopyStreamUtil.executeCopyIn(copyManager, copyInSql, inputStream);
                        } catch (SQLException | IOException e) {
                            throw new ProcessException("COPY IN failed", e);
                        }
                    });
                    inputRecordCount = result[0];
                } else {
                    // For transcoding records to Parquet - this approach is complex, will need
                    // separate implementation
                    throw new ProcessException("Record-to-Parquet transcoding not yet supported with streaming COPY approach");
                }
            } else {
                if (streamIncoming) {
                    // For streaming CSV files, stream directly from FlowFile to PostgreSQL
                    final long[] result = new long[1];
                    session.read(flowFile, inputStream -> {
                        try {
                            result[0] = CopyStreamUtil.executeCopyIn(copyManager, copyInSql, inputStream);
                        } catch (SQLException | IOException e) {
                            throw new ProcessException("COPY IN failed", e);
                        }
                    });
                    inputRecordCount = result[0];
                } else {
                    // Stream CSV data from records using writer function
                    final ProcessSession finalSession = session;
                    final FlowFile finalFlowFile = flowFile;
                    final RecordReaderFactory finalReaderFactory = readerFactory;
                    final ProcessorProperties finalProperties = properties;
                    inputRecordCount = CopyStreamUtil.executeCopyInWithWriter(copyManager, copyInSql, outputStream -> {
                        CsvImportStreamer.streamCsvFromRecords(finalSession, finalFlowFile, finalReaderFactory, finalProperties, outputStream,
                                getLogger());
                    });
                }
            }

            // Check if deduplication is enabled
            final boolean deduplicationEnabled = properties.isDeduplicationEnabled();
            final String timestampColumn = properties.getDeduplicationTimestampColumn();
            
            // If deduplication is enabled and we have JSONB columns, ensure jsonb_merge_agg function exists
            if (deduplicationEnabled && !metadata.getJsonbColumns().isEmpty()) {
                try {
                    SqlBuilder.ensureJsonbMergeAggExists(connection);
                    getLogger().debug("Ensured jsonb_merge_agg aggregate function exists");
                } catch (Exception e) {
                    throw new ProcessException("Failed to create jsonb_merge_agg aggregate function", e);
                }
            }
            
            // Generate upsert statement - only include RETURNING if we need rows back
            // If returning rows, always cast JSONB to TEXT for consistent text representation in both Parquet and CSV
            final boolean castJsonb = upsertReturnsRecords && !metadata.getJsonbColumns().isEmpty();
            final String exportQuery;
            if (deduplicationEnabled) {
                if (timestampColumn == null || timestampColumn.isBlank()) {
                    throw new ProcessException("Deduplication Timestamp Column is required when Enable Deduplication is true");
                }
                exportQuery = SqlBuilder.buildUpsertStatementWithDeduplication(metadata, schema, table, tempTable, 
                        timestampColumn, upsertReturnsRecords, castJsonb);
                getLogger().debug("Generated upsert SQL with deduplication (timestampCol={}, returnRows={}, castJsonb={}): {}", 
                        timestampColumn, upsertReturnsRecords, castJsonb, exportQuery);
            } else {
                exportQuery = SqlBuilder.buildUpsertStatement(metadata, schema, table, tempTable, upsertReturnsRecords, castJsonb);
                getLogger().debug("Generated upsert SQL (returnRows={}, castJsonb={}): {}", upsertReturnsRecords, castJsonb, exportQuery);
            }

            if (upsertReturnsRecords) {
                final String copyOutSql = sqlBuilder.buildCopyFromStatementSql(exportQuery);

                final CopyStreamUtil.CopyOutContext copyOutContext = CopyStreamUtil.startCopyOut(copyManager, copyOutSql,
                        "nifi-pg-bulkupsert-result-export");

                final RecordSetWriterFactory outWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
                if (maxRowsPerFlowFile <= 0) {
                    final Map<String, String> attributes = new HashMap<>();
                    if ("Parquet".equalsIgnoreCase(format)) {
                        flowFile = ParquetExportStreamer.transcodeParquetExportToWriter(session, flowFile, copyOutContext.getInputStream(),
                                outWriterFactory, getLogger(), attributes, null);
                    } else {
                        flowFile = CsvExportStreamer.writeCsvExportToFlowFile(session, flowFile, properties, outWriterFactory,
                                copyOutContext.getInputStream(), getLogger(), attributes, null);
                    }
                    flowFile = session.putAllAttributes(flowFile, attributes);
                } else {
                    // Chunk results into multiple FlowFiles
                    if ("Parquet".equalsIgnoreCase(format)) {
                        final List<FlowFile> parquetParts = ParquetExportStreamer.splitParquetBytesToFlowFiles(session, flowFile,
                                copyOutContext.getInputStream(), getLogger(), maxRowsPerFlowFile, null);
                        for (FlowFile p : parquetParts) {
                            session.transfer(p, REL_SUCCESS);
                        }
                    } else {
                        // CSV chunking via shared streamer
                        try {
                            final List<FlowFile> csvParts = CsvExportStreamer.writeCsvExportChunked(session, flowFile, properties, outWriterFactory,
                                    copyOutContext.getInputStream(), getLogger(), maxRowsPerFlowFile, null);
                            for (FlowFile p : csvParts) {
                                session.transfer(p, REL_SUCCESS);
                            }
                        } catch (Exception e) {
                            throw new ProcessException("Failed while chunking CSV upsert results", e);
                        }
                    }
                }
                try {
                    copyOutContext.joinAndRethrowIfError();
                } catch (RuntimeException e) {
                    throw new ProcessException("Error while exporting result stream", e.getCause() == null ? e : e.getCause());
                }
            } else {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(exportQuery);
                }
            }

            // Commit the transaction
            try {
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            } catch (SQLException e) {
                throw new ProcessException("Commit failed after UPSERT", e);
            }

            if (!"Parquet".equalsIgnoreCase(format) && !("CSV".equalsIgnoreCase(format) && properties.isStreamIncomingFile())) {
                flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(inputRecordCount));
            }
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            // Rollback the transaction on error
            rollbackConnection(connection);

            getLogger().error("Failed to bulk upsert using PostgreSQL", e);
            try {
                if (flowFile != null) {
                    // Get the most specific error message from the exception chain
                    String errorMessage = e.getMessage();
                    Throwable cause = e.getCause();
                    if (cause != null && cause.getMessage() != null) {
                        errorMessage = cause.getMessage();
                    }
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));
                    flowFile = session.putAttribute(flowFile, "pg.error", String.valueOf(errorMessage));
                    flowFile = session.putAttribute(flowFile, "failure.reason", String.valueOf(errorMessage));
                    flowFile = session.putAttribute(flowFile, "failure.stacktrace", sw.toString());
                }
            } catch (Exception ignored) {
                // Exception ignored
            }
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        } finally {
            // Restore original autoCommit state before connection returns to pool
            restoreAutoCommit(connection, originalAutoCommit);
        }
        sqlBuilder.cleanup();
    }

    /**
     * Rollback a database transaction. Follows NiFi standard pattern for connection management.
     */
    private void rollbackConnection(Connection connection) {
        if (connection != null) {
            try {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                    getLogger().debug("Rolled back JDBC transaction");
                }
            } catch (final SQLException rollbackException) {
                getLogger().error("Failed to rollback JDBC transaction", rollbackException);
            }
        }
    }

    /**
     * Restore the original autoCommit state before returning connection to pool. Follows NiFi standard pattern for connection management.
     */
    private void restoreAutoCommit(Connection connection, boolean originalAutoCommit) {
        if (connection != null) {
            try {
                if (originalAutoCommit && !connection.getAutoCommit()) {
                    connection.setAutoCommit(originalAutoCommit);
                }
            } catch (final SQLException autoCommitException) {
                getLogger().warn("Failed to restore auto-commit to {}", originalAutoCommit, autoCommitException);
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final boolean streamIncoming = validationContext.getProperty(STREAM_INCOMING_FILE).evaluateAttributeExpressions().asBoolean();
        if (!streamIncoming) {
            if (!validationContext.getProperty(RECORD_READER).isSet()) {
                results.add(new ValidationResult.Builder().subject("Record Reader").valid(false)
                        .explanation("Record Reader is required when not streaming the incoming file").build());
            }
        }
        final boolean returns = validationContext.getProperty(UPSERT_RETURNS_RECORDS).evaluateAttributeExpressions().asBoolean();
        if (returns && !validationContext.getProperty(RECORD_WRITER).isSet()) {
            results.add(new ValidationResult.Builder().subject("Record Writer").valid(false)
                    .explanation("Record Writer is required when exporting results").build());
        }
        final boolean deduplicationEnabled = validationContext.getProperty(ENABLE_DEDUPLICATION).evaluateAttributeExpressions().asBoolean();
        if (deduplicationEnabled) {
            final String timestampColumn = validationContext.getProperty(DEDUPLICATION_TIMESTAMP_COLUMN)
                    .evaluateAttributeExpressions().getValue();
            if (timestampColumn == null || timestampColumn.isBlank()) {
                results.add(new ValidationResult.Builder().subject("Deduplication Timestamp Column").valid(false)
                        .explanation("Deduplication Timestamp Column is required when Enable Deduplication is true").build());
            }
        }
        return results;
    }

    private String getTargetColumnsCsv(final ProcessorProperties properties) {
        final String cols = properties.getCsvTargetColumns();
        if (cols == null || cols.isBlank())
            return "";
        final String[] parts = cols.split(",");
        final StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            final String c = p.trim();
            if (c.isEmpty())
                continue;
            if (sb.length() > 0)
                sb.append(", ");
            sb.append('"').append(c.replace("\"", "\"\"")).append('"');
        }
        return sb.toString();
    }

    private List<String> resolveHeaderFields(final ProcessorProperties properties, final RecordSchema schema) {
        final String cols = properties.getCsvTargetColumns();
        if (cols == null || cols.isBlank()) {
            final List<String> fieldNames = new ArrayList<>();
            for (RecordField f : schema.getFields()) {
                fieldNames.add(f.getFieldName());
            }
            return fieldNames;
        }
        final String[] parts = cols.split(",");
        final List<String> ordered = new ArrayList<>(parts.length);
        for (String p : parts) {
            final String name = p.trim();
            if (!name.isEmpty())
                ordered.add(name);
        }
        return ordered;
    }

    private String quoteIdentifier(final String identifier) {
        if (identifier == null || identifier.isBlank())
            return identifier;
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private String quoteIdentifierPath(final String path) {
        if (path == null || path.isBlank())
            return path;
        final String[] parts = path.split("\\.");
        final StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            if (sb.length() > 0)
                sb.append('.');
            sb.append(quoteIdentifier(p.trim()));
        }
        return sb.toString();
    }

    private boolean isParquetReader(final RecordReaderFactory factory) {
        return factory != null && factory.getClass().getSimpleName().toLowerCase().contains("parquet");
    }

    private boolean isParquetWriter(final RecordSetWriterFactory factory) {
        return factory != null && factory.getClass().getSimpleName().toLowerCase().contains("parquet");
    }

    private void transcodeToParquetCopyIn(final ProcessSession session, final FlowFile flowFile, final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory, final OutputStream out) {
        session.read(flowFile, in -> {
            try (RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
                    RecordSetWriter writer = writerFactory.createWriter(getLogger(), reader.getSchema(), out, new HashMap<>())) {
                writer.beginRecordSet();
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    writer.write(record);
                }
                writer.finishRecordSet();
            } catch (Exception e) {
                throw new ProcessException("Failed to transcode input to Parquet for COPY FROM", e);
            }
        });
    }
}
