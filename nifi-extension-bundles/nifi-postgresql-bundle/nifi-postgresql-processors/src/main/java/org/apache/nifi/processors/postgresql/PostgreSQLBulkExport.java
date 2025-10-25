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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.csv.CSVRecordReader;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.postgresql.service.PostgreSQLConnectionPool;
import org.apache.nifi.postgresql.service.util.TableMetadata;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionWrapper;
import org.apache.nifi.processors.postgresql.stream.CsvExportStreamer;
import org.apache.nifi.processors.postgresql.stream.ParquetExportStreamer;
import org.apache.nifi.processors.postgresql.util.CopyStreamUtil;
import org.apache.nifi.processors.postgresql.util.CsvFormats;
import org.apache.nifi.processors.postgresql.util.MaxValueTracker;
import org.apache.nifi.processors.postgresql.util.ProcessorProperties;
import org.apache.nifi.processors.postgresql.util.SqlBuilder;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

@Tags({"postgresql", "copy to", "bulk", "export", "incremental", "state"})
@CapabilityDescription("Exports rows from PostgreSQL using COPY (query) TO STDOUT with CSV HEADER. "
        + "Supports initial-load and incremental fetching using maximum-value columns with persisted state and optional chunking into multiple FlowFiles. "
        + "Parses the COPY CSV stream using Apache Commons CSV and writes records using a configured RecordSetWriter. "
        + "CSV options (delimiter, quote, escape, NULL token) are configurable. Creates/drops a quoted temporary view to wrap custom queries. "
        + "Sets 'record.count' and 'mime.type' attributes on results.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@SupportsBatching
@PrimaryNodeOnly
@Stateful(scopes = Scope.CLUSTER,
        description = "Stores maximum observed values for configured columns to support incremental fetching across cluster nodes.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@WritesAttributes({@WritesAttribute(attribute = "mime.type", description = "MIME type of the output content"),
        @WritesAttribute(attribute = "record.count", description = "Number of records written when conversion occurs")})
/**
 * Exports rows from PostgreSQL using COPY (query) TO STDOUT. Supports incremental fetching via maximum-value columns persisted in state and optional
 * chunking of results.
 */
public class PostgreSQLBulkExport extends AbstractProcessor {

    static final PropertyDescriptor CONNECTION_PROVIDER = ProcessorProperties.CONNECTION_PROVIDER;
    static final PropertyDescriptor BULK_TRANSFER_DATA_FORMAT = ProcessorProperties.BULK_TRANSFER_DATA_FORMAT;

    // Table or query options
    static final PropertyDescriptor SOURCE_TABLE = ProcessorProperties.SOURCE_TABLE;
    static final PropertyDescriptor CUSTOM_QUERY = ProcessorProperties.CUSTOM_QUERY;

    // Chunking options (only CSV)
    static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = ProcessorProperties.MAX_ROWS_PER_FLOW_FILE;
    static final PropertyDescriptor MAXIMUM_VALUE_COLUMNS = ProcessorProperties.MAXIMUM_VALUE_COLUMNS;

    // CSV options for result export (only CSV)
    static final PropertyDescriptor RECORD_WRITER = ProcessorProperties.RECORD_WRITER;
    static final PropertyDescriptor CSV_DELIMITER = ProcessorProperties.CSV_DELIMITER;
    static final PropertyDescriptor CSV_QUOTE = ProcessorProperties.CSV_QUOTE;
    static final PropertyDescriptor CSV_ESCAPE = ProcessorProperties.CSV_ESCAPE;
    static final PropertyDescriptor CSV_NULL = ProcessorProperties.CSV_NULL;
    static final PropertyDescriptor CSV_HEADER = ProcessorProperties.CSV_HEADER;

    // Parquet (COPY TO) options
    static final PropertyDescriptor PARQUET_VERSION = ProcessorProperties.PARQUET_VERSION;

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("FlowFiles containing exported data").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles that failed during export").build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(CONNECTION_PROVIDER, BULK_TRANSFER_DATA_FORMAT, SOURCE_TABLE, CUSTOM_QUERY,
            MAX_ROWS_PER_FLOW_FILE, MAXIMUM_VALUE_COLUMNS, RECORD_WRITER, CSV_DELIMITER, CSV_QUOTE, CSV_ESCAPE, CSV_NULL, CSV_HEADER,
            PARQUET_VERSION);

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
        final FlowFile working = sqlBuilder.getWorkingFlowFile();
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

            final String viewName = "nifi_export_view_" + UUID.randomUUID().toString().replaceAll("-", "");
            final String baseQuery = sqlBuilder.buildBaseQuery();
            final String emptyViewSql = sqlBuilder.buildEmptyViewFromStatementSql(viewName, baseQuery);
            createTempView(wrapper, connection, emptyViewSql);
            try {
                final String format = properties.getBulkTransferDataFormat();
                final RecordSchema writeSchema = "CSV".equalsIgnoreCase(format) ? fetchViewSchema(connection, viewName) : null;

                // Cast JSONB columns to TEXT for both Parquet and CSV to ensure consistent text representation
                // Use metadata service to get JSONB columns with caching
                final PostgreSQLConnectionPool pool = (PostgreSQLConnectionPool) connectionProvider;
                // Fetch metadata for the temp view - note: views appear as tables in pg_class
                final TableMetadata metadata = pool.getTableMetadata("pg_temp", viewName);
                final List<String> jsonbColumns = new ArrayList<>(metadata.getJsonbColumns());
                
                final String exportQuery = jsonbColumns.isEmpty()
                        ? baseQuery
                        : SqlBuilder.wrapQueryWithJsonbCasting(connection, viewName, baseQuery, jsonbColumns);

                final String copySql = sqlBuilder.buildCopyFromStatementSql(exportQuery);
                final List<String> maxColumns = sqlBuilder.getMaximumValueColumns();
                final MaxValueTracker maxTracker = new MaxValueTracker(maxColumns);
                // Seed initial max values from dynamic properties if state empty
                if (!maxColumns.isEmpty()) {
                    final Map<String, String> state = properties.getLastMaxValuesFromState();
                    if (state.isEmpty()) {
                        final Map<String, String> initial = properties.getInitialMaxValuesFromDynamicProperties();
                        if (!initial.isEmpty()) {
                            final Map<String, String> newState = new HashMap<>();
                            for (Map.Entry<String, String> e : initial.entrySet()) {
                                newState.put(properties.getQualifiedStateKey(e.getKey()), e.getValue());
                            }
                            if (!newState.isEmpty()) {
                                try {
                                    context.getStateManager().setState(newState, Scope.CLUSTER);
                                } catch (IOException ioe) {
                                    throw new ProcessException("Failed to store initial max values", ioe);
                                }
                            }
                        }
                    }
                }

                final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
                final int maxRows = properties.getMaxRowsPerFlowFile();
                final boolean chunking = maxRows > 0;
                if (!chunking || (format != null && format.equalsIgnoreCase("Parquet"))) {
                    final CopyStreamUtil.CopyOutContext copyOutContext = CopyStreamUtil.startCopyOut(copyManager, copySql,
                            "nifi-pg-bulkexport-export");

                    final Map<String, String> attributes = new HashMap<>();
                    if ("CSV".equalsIgnoreCase(format)) {
                        FlowFile output = session.create();
                        output = CsvExportStreamer.writeCsvExportToFlowFileWithSchema(session, output, properties, writerFactory, writeSchema,
                                copyOutContext.getInputStream(), getLogger(), attributes, maxTracker);
                        try {
                            copyOutContext.joinAndRethrowIfError();
                        } catch (RuntimeException e) {
                            throw new ProcessException("COPY TO STDOUT failed", e.getCause() == null ? e : e.getCause());
                        }
                        // Update state and add maxvalue.* attributes to FlowFile
                        final Map<String, String> stateAttrs = writeStateAndReturnFlowAttributes(context, properties, maxTracker);
                        if (!stateAttrs.isEmpty()) {
                            attributes.putAll(stateAttrs);
                        }
                        if (!attributes.isEmpty())
                            output = session.putAllAttributes(output, attributes);
                        session.transfer(output, REL_SUCCESS);
                    } else { // Parquet
                        if (maxRows <= 0) {
                            FlowFile output = session.create();
                            output = ParquetExportStreamer.writeParquetBytesToFlowFile(session, output, copyOutContext.getInputStream(), getLogger(),
                                    attributes, maxTracker);
                            try {
                                copyOutContext.joinAndRethrowIfError();
                            } catch (RuntimeException e) {
                                throw new ProcessException("COPY TO STDOUT failed", e.getCause() == null ? e : e.getCause());
                            }
                            final Map<String, String> stateAttrs = writeStateAndReturnFlowAttributes(context, properties, maxTracker);
                            if (!stateAttrs.isEmpty())
                                attributes.putAll(stateAttrs);
                            if (!attributes.isEmpty())
                                output = session.putAllAttributes(output, attributes);
                            session.transfer(output, REL_SUCCESS);
                        } else {
                            // Use splitParquetBytesToFlowFiles which internally handles row count check and
                            // splitting
                            // Note: This method observes records for maxTracker when file is small enough
                            final List<FlowFile> parts = ParquetExportStreamer.splitParquetBytesToFlowFiles(session, working,
                                    copyOutContext.getInputStream(), getLogger(), maxRows, maxTracker);
                            try {
                                copyOutContext.joinAndRethrowIfError();
                            } catch (RuntimeException e) {
                                throw new ProcessException("COPY TO STDOUT failed", e.getCause() == null ? e : e.getCause());
                            }
                            final Map<String, String> stateAttrs = writeStateAndReturnFlowAttributes(context, properties, maxTracker);
                            for (FlowFile p : parts) {
                                FlowFile out = p;
                                if (!stateAttrs.isEmpty())
                                    out = session.putAllAttributes(out, stateAttrs);
                                session.transfer(out, REL_SUCCESS);
                            }
                            sqlBuilder.cleanup();
                            return;
                        }
                    }
                } else {
                    if (format != null && format.equalsIgnoreCase("Parquet")) {
                        getLogger().warn("Max Rows Per Flow File is not supported for Parquet export; creating a single FlowFile.");
                    }
                    final CopyStreamUtil.CopyOutContext copyOutContext2 = CopyStreamUtil.startCopyOut(copyManager, copySql,
                            "nifi-pg-bulkexport-export");
                    try {
                        final CSVFormat csvFormat = CsvFormats.buildCsvParseFormat(properties);
                        try (CSVRecordReader rr = new CSVRecordReader(copyOutContext2.getInputStream(), getLogger(), writeSchema, csvFormat, true,
                                false, null, null, null, StandardCharsets.UTF_8.name())) {
                            List<Record> batch = new ArrayList<>(maxRows);
                            Record rec;
                            while ((rec = rr.nextRecord()) != null) {
                                if (maxTracker != null)
                                    maxTracker.observe(rec);
                                batch.add(rec);
                                if (batch.size() == maxRows) {
                                    FlowFile part = session.create();
                                    final Map<String, String> attributes = new HashMap<>();
                                    part = session.write(part, out -> {
                                        try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, attributes)) {
                                            writer.beginRecordSet();
                                            for (Record row : batch) {
                                                writer.write(row);
                                            }
                                            final WriteResult writeResult = writer.finishRecordSet();
                                            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                                        } catch (SchemaNotFoundException | IOException e) {
                                            throw new ProcessException("Failed while writing records", e);
                                        }
                                    });
                                    final Map<String, String> stateAttrs = writeStateAndReturnFlowAttributes(context, properties, maxTracker);
                                    if (!stateAttrs.isEmpty())
                                        attributes.putAll(stateAttrs);
                                    part = session.putAllAttributes(part, attributes);
                                    session.transfer(part, REL_SUCCESS);
                                    batch.clear();
                                }
                            }
                            if (!batch.isEmpty()) {
                                FlowFile part = session.create();
                                final Map<String, String> attributes = new HashMap<>();
                                part = session.write(part, out -> {
                                    try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, attributes)) {
                                        writer.beginRecordSet();
                                        for (Record row : batch) {
                                            writer.write(row);
                                        }
                                        final WriteResult writeResult = writer.finishRecordSet();
                                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                                    } catch (SchemaNotFoundException | IOException e) {
                                        throw new ProcessException("Failed while writing records", e);
                                    }
                                });
                                final Map<String, String> stateAttrs = writeStateAndReturnFlowAttributes(context, properties, maxTracker);
                                if (!stateAttrs.isEmpty())
                                    attributes.putAll(stateAttrs);
                                part = session.putAllAttributes(part, attributes);
                                session.transfer(part, REL_SUCCESS);
                            }
                        }
                    } finally {
                        try {
                            copyOutContext2.joinAndRethrowIfError();
                        } catch (RuntimeException e) {
                            throw new ProcessException("COPY TO STDOUT failed", e.getCause() == null ? e : e.getCause());
                        }
                    }
                    // State already updated per part
                    sqlBuilder.cleanup();
                    return;
                }
            } finally {
                dropTempViewQuietly(connection, viewName);
            }
        } catch (Exception e) {
            // Rollback the transaction on error
            rollbackConnection(connection);

            getLogger().error("Failed to bulk export from PostgreSQL: {}", e.getMessage(), e);

            // Create failure FlowFile with detailed error information
            FlowFile failureFlowFile = session.create();
            failureFlowFile = session.putAttribute(failureFlowFile, "failure.reason", e.getClass().getSimpleName() + ": " + e.getMessage());

            // Add stack trace for debugging
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            failureFlowFile = session.putAttribute(failureFlowFile, "failure.stacktrace", sw.toString());

            session.transfer(failureFlowFile, REL_FAILURE);
            return;
        } finally {
            // Restore original autoCommit state before connection returns to pool
            restoreAutoCommit(connection, originalAutoCommit);
            sqlBuilder.cleanup();
        }
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

    private void createTempView(final PostgreSQLConnectionWrapper wrapper, final Connection connection, final String sql) {
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        } catch (Exception e) {
            throw new ProcessException("Failed to create temp view for export", e);
        }
    }

    private void dropTempViewQuietly(final Connection connection, final String viewName) {
        try (Statement st = connection.createStatement()) {
            st.execute("DROP VIEW IF EXISTS " + SqlBuilder.quoteIdentifier(viewName));
        } catch (Exception ignored) {
            // Exception ignored - cleanup operation
        }
    }

    private RecordSchema fetchViewSchema(final Connection connection, final String viewName) {
        final List<RecordField> fields = new ArrayList<>();
        final String sql = "select a.attname, format_type(a.atttypid, a.atttypmod) typ " + "from pg_attribute a join pg_class c on a.attrelid=c.oid "
                + "where c.relname='" + viewName + "' and a.attnum>0 and not a.attisdropped order by a.attnum";
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                final String col = rs.getString(1);
                final String typ = rs.getString(2).toLowerCase();
                fields.add(new RecordField(col, mapPgTypeToRecordType(typ)));
            }
        } catch (Exception e) {
            throw new ProcessException("Failed to fetch schema from temp view", e);
        }
        return new SimpleRecordSchema(fields);
    }

    private DataType mapPgTypeToRecordType(final String pgType) {
        if (pgType.startsWith("int2") || pgType.equals("smallint"))
            return RecordFieldType.SHORT.getDataType();
        if (pgType.startsWith("int4") || pgType.equals("integer"))
            return RecordFieldType.INT.getDataType();
        if (pgType.startsWith("int8") || pgType.equals("bigint"))
            return RecordFieldType.LONG.getDataType();
        if (pgType.startsWith("float4") || pgType.equals("real"))
            return RecordFieldType.FLOAT.getDataType();
        if (pgType.startsWith("float8") || pgType.equals("double precision"))
            return RecordFieldType.DOUBLE.getDataType();
        if (pgType.startsWith("numeric") || pgType.startsWith("decimal"))
            return RecordFieldType.DECIMAL.getDataType();
        if (pgType.equals("boolean"))
            return RecordFieldType.BOOLEAN.getDataType();
        if (pgType.equals("date"))
            return RecordFieldType.DATE.getDataType();
        if (pgType.startsWith("timestamp"))
            return RecordFieldType.TIMESTAMP.getDataType();
        if (pgType.equals("time"))
            return RecordFieldType.TIME.getDataType();
        if (pgType.equals("uuid"))
            return RecordFieldType.UUID.getDataType();
        return RecordFieldType.STRING.getDataType();
    }

    private Map<String, Object> parseCsvRecord(final CSVRecord csvRecord, final RecordSchema writeSchema, final String nullToken) {
        final Map<String, Object> out = new HashMap<>();
        for (RecordField f : writeSchema.getFields()) {
            final String name = f.getFieldName();
            final String raw = csvRecord.isMapped(name) ? csvRecord.get(name) : null;
            if (raw == null || (nullToken != null && nullToken.equals(raw))) {
                out.put(name, null);
                continue;
            }
            switch (f.getDataType().getFieldType()) {
                case INT -> out.put(name, Integer.valueOf(raw));
                case LONG -> out.put(name, Long.valueOf(raw));
                case SHORT -> out.put(name, Short.valueOf(raw));
                case FLOAT -> out.put(name, Float.valueOf(raw));
                case DOUBLE -> out.put(name, Double.valueOf(raw));
                case DECIMAL -> out.put(name, new BigDecimal(raw));
                case BOOLEAN -> out.put(name, Boolean.valueOf(raw));
                case DATE -> out.put(name, Date.valueOf(raw));
                case TIME -> out.put(name, Time.valueOf(raw));
                case TIMESTAMP -> out.put(name, Timestamp.valueOf(raw.replace('T', ' ').replace('Z', ' ')));
                case UUID -> out.put(name, UUID.fromString(raw));
                default -> out.put(name, raw);
            }
        }
        return out;
    }

    private void updateStateFromTracker(final ProcessContext context, final MaxValueTracker maxTracker) {
        if (maxTracker == null)
            return;
        final Map<String, String> newState = new HashMap<>();
        final ProcessorProperties props = new ProcessorProperties(context, null);
        for (Map.Entry<String, String> e : maxTracker.getMaxValuesAsStrings().entrySet()) {
            newState.put(props.getQualifiedStateKey(e.getKey()), e.getValue());
        }
        if (!newState.isEmpty()) {
            try {
                context.getStateManager().setState(newState, Scope.CLUSTER);
            } catch (IOException e) {
                throw new ProcessException("Failed to store processor state", e);
            }
        }
    }

    private Map<String, String> writeStateAndReturnFlowAttributes(final ProcessContext context, final ProcessorProperties props,
            final MaxValueTracker maxTracker) {
        if (maxTracker == null)
            return Collections.emptyMap();
        final Map<String, String> attrs = new HashMap<>();
        final Map<String, String> newState = new HashMap<>();
        for (Map.Entry<String, String> e : maxTracker.getMaxValuesAsStrings().entrySet()) {
            final String key = props.getQualifiedStateKey(e.getKey());
            newState.put(key, e.getValue());
            attrs.put("maxvalue." + e.getKey(), e.getValue());
        }
        if (!newState.isEmpty()) {
            try {
                context.getStateManager().setState(newState, Scope.CLUSTER);
            } catch (IOException e) {
                throw new ProcessException("Failed to store processor state", e);
            }
        }
        return attrs;
    }

    // Removed sessionFactoryForState

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        // Writer is always required for outgoing FlowFile
        if (!validationContext.getProperty(RECORD_WRITER).isSet()) {
            results.add(new ValidationResult.Builder().subject("Record Writer").valid(false)
                    .explanation("Record Writer is required for exporting results").build());
        }
        return results;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName != null && propertyDescriptorName.startsWith("initial.maxvalue.")) {
            return new PropertyDescriptor.Builder().name(propertyDescriptorName).required(false).dynamic(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .build();
        }
        return null;
    }
}
