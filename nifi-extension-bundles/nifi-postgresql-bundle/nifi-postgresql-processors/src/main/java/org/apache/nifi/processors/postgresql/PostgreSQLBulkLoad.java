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
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReaderFactory;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.processors.postgresql.stream.CsvImportStreamer;
import org.apache.nifi.processors.postgresql.stream.ParquetImportStreamer;
import org.apache.nifi.processors.postgresql.util.CopyStreamUtil;
import org.apache.nifi.processors.postgresql.util.CsvFormats;
import org.apache.nifi.processors.postgresql.util.ProcessorProperties;
import org.apache.nifi.processors.postgresql.util.SqlBuilder;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Loads FlowFile records into a PostgreSQL table using COPY FROM STDIN in CSV or Parquet format.
 * Supports streaming raw bytes or transcoding via RecordReader/RecordSetWriter.
 */
@Tags({"postgresql", "copy from", "bulk", "ingest", "load"})
@CapabilityDescription("Loads FlowFile records into a PostgreSQL table using COPY FROM STDIN (CSV or Parquet). In CSV mode, a RecordReader parses incoming FlowFiles and Apache Commons CSV generates the COPY stream. In Parquet mode, the FlowFile content is streamed to COPY FROM via pg_parquet with optional MATCH_BY. Supports specifying an ordered column list for CSV mode and writes 'record.count' for CSV inputs.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records written to PostgreSQL")
})
public class PostgreSQLBulkLoad extends AbstractProcessor {

    static final PropertyDescriptor CONNECTION_PROVIDER = ProcessorProperties.CONNECTION_PROVIDER;
    static final PropertyDescriptor TARGET_TABLE = ProcessorProperties.TARGET_TABLE;
    static final PropertyDescriptor BULK_TRANSFER_DATA_FORMAT = ProcessorProperties.BULK_TRANSFER_DATA_FORMAT;
    static final PropertyDescriptor STREAM_INCOMING_FILE = ProcessorProperties.STREAM_INCOMING_FILE;
    static final PropertyDescriptor RECORD_READER = ProcessorProperties.RECORD_READER;
    static final PropertyDescriptor RECORD_WRITER = ProcessorProperties.RECORD_WRITER;
    static final PropertyDescriptor TARGET_COLUMNS = ProcessorProperties.TARGET_COLUMNS;
    static final PropertyDescriptor CSV_DELIMITER = ProcessorProperties.CSV_DELIMITER;
    static final PropertyDescriptor CSV_QUOTE = ProcessorProperties.CSV_QUOTE;
    static final PropertyDescriptor CSV_ESCAPE = ProcessorProperties.CSV_ESCAPE;
    static final PropertyDescriptor CSV_NULL = ProcessorProperties.CSV_NULL;
    static final PropertyDescriptor CSV_HEADER = ProcessorProperties.CSV_HEADER;
    static final PropertyDescriptor PARQUET_MATCH_BY = ProcessorProperties.PARQUET_MATCH_BY;

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles successfully loaded into the target table")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be loaded")
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_PROVIDER,
            TARGET_TABLE,
            BULK_TRANSFER_DATA_FORMAT,
            STREAM_INCOMING_FILE,
            RECORD_READER,
            RECORD_WRITER,
            TARGET_COLUMNS,
            CSV_DELIMITER,
            CSV_QUOTE,
            CSV_ESCAPE,
            CSV_NULL,
            CSV_HEADER,
            PARQUET_MATCH_BY
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

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

        final String tableName = properties.getTargetTable();
        final String format = properties.getBulkTransferDataFormat();
        final PostgreSQLConnectionProviderService connectionProvider =
                context.getProperty(CONNECTION_PROVIDER).asControllerService(PostgreSQLConnectionProviderService.class);

        try (PostgreSQLConnectionWrapper wrapper = connectionProvider.getPostgreSQLConnection()) {
            final PGConnection pgConnection = wrapper.unwrap();
            try { wrapper.connection.setAutoCommit(false); } catch (Exception e) { throw new ProcessException("Failed to disable auto-commit", e); }
            final CopyManager copyManager = pgConnection.getCopyAPI();

            final String copySql;
            final boolean streamIncoming = properties.isStreamIncomingFile();
            final RecordReaderFactory readerFactory = streamIncoming
                    ? null
                    : context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            if ("Parquet".equalsIgnoreCase(format)) {
                copySql = sqlBuilder.buildCopyIntoTableSql();
            } else {
                final String targetColumnsCsv = getTargetColumnsCsv(properties);
                copySql = sqlBuilder.buildCopyIntoTableSql(targetColumnsCsv);
            }

            // Execute COPY IN operation using true streaming approach (following PostgreSQL JDBC documentation)
            final long recordCount;
            if ("Parquet".equalsIgnoreCase(format)) {
                if (streamIncoming) {
                    // For streaming Parquet files, stream directly from FlowFile to PostgreSQL
                    final long[] result = new long[1];
                    session.read(flowFile, inputStream -> {
                        try {
                            result[0] = CopyStreamUtil.executeCopyIn(copyManager, copySql, inputStream);
                        } catch (SQLException | IOException e) {
                            throw new ProcessException("COPY IN failed", e);
                        }
                    });
                    recordCount = result[0];
                } else {
                    // For transcoding records to Parquet - this approach is complex, will need separate implementation
                    throw new ProcessException("Record-to-Parquet transcoding not yet supported with streaming COPY approach");
                }
            } else {
                if (streamIncoming) {
                    // For streaming CSV files, stream directly from FlowFile to PostgreSQL
                    final long[] result = new long[1];
                    session.read(flowFile, inputStream -> {
                        try {
                            result[0] = CopyStreamUtil.executeCopyIn(copyManager, copySql, inputStream);
                        } catch (SQLException | IOException e) {
                            throw new ProcessException("COPY IN failed", e);
                        }
                    });
                    recordCount = result[0];
                } else {
                    // Stream CSV data from records using writer function
                    final ProcessSession finalSession = session;
                    final FlowFile finalFlowFile = flowFile;
                    final RecordReaderFactory finalReaderFactory = readerFactory;
                    final ProcessorProperties finalProperties = properties;
                    recordCount = CopyStreamUtil.executeCopyInWithWriter(copyManager, copySql, outputStream -> {
                        CsvImportStreamer.streamCsvFromRecords(finalSession, finalFlowFile, finalReaderFactory, finalProperties, outputStream, getLogger());
                    });
                }
            }
            

            try { wrapper.connection.commit(); } catch (Exception e) { throw new ProcessException("Commit failed after COPY", e); }

            if (!"Parquet".equalsIgnoreCase(format) && !streamIncoming) flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(recordCount));
            session.getProvenanceReporter().send(flowFile, "postgresql:" + SqlBuilder.quoteIdentifierPath(tableName));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to bulk load into PostgreSQL", e);
            try {
                if (flowFile != null) {
                    // Get the most specific error message from the exception chain
                    String errorMessage = e.getMessage();
                    Throwable cause = e.getCause();
                    if (cause != null && cause.getMessage() != null) {
                        errorMessage = cause.getMessage();
                    }
                    flowFile = session.putAttribute(flowFile, "pg.error", String.valueOf(errorMessage));
                }
            } catch (Exception ignore) { }
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
        sqlBuilder.cleanup();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final String format = validationContext.getProperty(BULK_TRANSFER_DATA_FORMAT).getValue();
        final boolean streamIncoming = validationContext.getProperty(STREAM_INCOMING_FILE).evaluateAttributeExpressions().asBoolean();
        if (!streamIncoming) {
            if (!validationContext.getProperty(RECORD_READER).isSet()) {
                results.add(new ValidationResult.Builder()
                        .subject("Record Reader")
                        .valid(false)
                        .explanation("Record Reader is required when not streaming the incoming file")
                        .build());
            }
        }
        return results;
    }

    private String getTargetColumnsCsv(final ProcessorProperties properties) {
        final String cols = properties.getTargetColumns();
        if (cols == null || cols.isBlank()) return "";
        final String[] parts = cols.split(",");
        final StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            final String c = p.trim();
            if (c.isEmpty()) continue;
            if (sb.length() > 0) sb.append(", ");
            sb.append('"').append(c.replace("\"", "\"\"")).append('"');
        }
        return sb.toString();
    }

    private List<String> resolveHeaderFields(final ProcessorProperties properties, final RecordSchema schema) {
        final String cols = properties.getTargetColumns();
        if (cols == null || cols.isBlank()) {
            final List<String> fieldNames = new ArrayList<>();
            for (RecordField field : schema.getFields()) {
                fieldNames.add(field.getFieldName());
            }
            return fieldNames;
        }
        final String[] parts = cols.split(",");
        final List<String> ordered = new ArrayList<>(parts.length);
        for (String p : parts) {
            final String name = p.trim();
            if (!name.isEmpty()) ordered.add(name);
        }
        return ordered;
    }

    private boolean isParquetReader(final RecordReaderFactory factory) {
        return factory != null && factory.getClass().getSimpleName().toLowerCase().contains("parquet");
    }

    private boolean isParquetWriter(final RecordSetWriterFactory factory) {
        return factory != null && factory.getClass().getSimpleName().toLowerCase().contains("parquet");
    }

    // No local transcode method; using shared streamer
}


