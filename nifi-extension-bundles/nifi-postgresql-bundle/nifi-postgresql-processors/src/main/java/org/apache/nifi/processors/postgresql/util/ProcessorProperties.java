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

package org.apache.nifi.processors.postgresql.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.postgresql.PostgreSQLConnectionProviderService;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared PropertyDescriptors and per-invocation evaluated values for PostgreSQL processors.
 * Encapsulates common COPY and query properties and provides evaluated getters.
 */
public final class ProcessorProperties {
    // Instance-scoped evaluated properties and context
    private final ProcessContext context;
    private final ProcessSession session;
    private FlowFile workingFlowFile;
    private final boolean placeholderCreated;

    // Cached evaluated values
    private final String bulkTransferDataFormat;
    private final String targetTable;
    private final String upsertSqlTemplate;
    private final Boolean upsertReturnsRecords;

    private final String sourceTable;
    private final String customQuery;
    private final Integer maxRowsPerFlowFile;
    private final String maximumValueColumns;

    private final String csvTargetColumns;
    private final String targetColumns;
    private final String csvDelimiter;
    private final String csvQuote;
    private final String csvEscape;
    private final String csvNullToken;
    private final String csvHeader;

    private final String parquetMatchBy;
    private final String parquetVersion;

    // Streaming toggles
    private final Boolean streamIncomingFile;

    public ProcessorProperties(final ProcessContext context, final ProcessSession session) {
        this.context = context;
        this.session = session;

        FlowFile candidate = session.get();
        boolean created = false;
        if (candidate == null) {
            candidate = session.create();
            created = true;
        }
        this.workingFlowFile = candidate;
        this.placeholderCreated = created;

        // Evaluate and cache properties
        this.bulkTransferDataFormat = getPropertyValue(context, workingFlowFile, BULK_TRANSFER_DATA_FORMAT);
        this.targetTable = getPropertyValue(context, workingFlowFile, TARGET_TABLE);
        this.upsertSqlTemplate = getPropertyValue(context, workingFlowFile, UPSERT_SQL_TEMPLATE);
        final String upsertReturnsRecordsStr = getPropertyValue(context, workingFlowFile, UPSERT_RETURNS_RECORDS);
        this.upsertReturnsRecords = upsertReturnsRecordsStr == null ? null : Boolean.valueOf(upsertReturnsRecordsStr);

        this.sourceTable = getPropertyValue(context, workingFlowFile, SOURCE_TABLE);
        this.customQuery = getPropertyValue(context, workingFlowFile, CUSTOM_QUERY);
        this.maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).asInteger();
        this.maximumValueColumns = context.getProperty(MAXIMUM_VALUE_COLUMNS).getValue();

        // Conditionally evaluate CSV properties when CSV format selected
        if ("CSV".equalsIgnoreCase(this.bulkTransferDataFormat)) {
            this.csvTargetColumns = getPropertyValue(context, workingFlowFile, CSV_TARGET_COLUMNS);
            this.targetColumns = getPropertyValue(context, workingFlowFile, TARGET_COLUMNS);
            this.csvDelimiter = getPropertyValue(context, workingFlowFile, CSV_DELIMITER);
            this.csvQuote = getPropertyValue(context, workingFlowFile, CSV_QUOTE);
            this.csvEscape = getPropertyValue(context, workingFlowFile, CSV_ESCAPE);
            this.csvNullToken = getPropertyValue(context, workingFlowFile, CSV_NULL);
            this.csvHeader = getPropertyValue(context, workingFlowFile, CSV_HEADER);
        } else {
            this.csvTargetColumns = null;
            this.targetColumns = null;
            this.csvDelimiter = null;
            this.csvQuote = null;
            this.csvEscape = null;
            this.csvNullToken = null;
            this.csvHeader = null;
        }

        // Conditionally evaluate Parquet properties when Parquet format selected
        if ("Parquet".equalsIgnoreCase(this.bulkTransferDataFormat)) {
            this.parquetMatchBy = getPropertyValue(context, workingFlowFile, PARQUET_MATCH_BY);
            this.parquetVersion = getPropertyValue(context, workingFlowFile, PARQUET_VERSION);
        } else {
            this.parquetMatchBy = null;
            this.parquetVersion = null;
        }

        final String streamIn = getPropertyValue(context, workingFlowFile, STREAM_INCOMING_FILE);
        this.streamIncomingFile = streamIn == null ? null : Boolean.valueOf(streamIn);

    }

    // Backward-compatible static helper for other modules not yet migrated
    public static String getPropertyValue(final ProcessContext context, final FlowFile flowFile, final PropertyDescriptor property) {
        final String value = context.getProperty(property).evaluateAttributeExpressions(flowFile).getValue();
        if (value == null || value.isBlank()) {
            return null;
        }
        return value;
    }

    public ProcessContext getContext() { return context; }
    public FlowFile getWorkingFlowFile() { return workingFlowFile; }
    public boolean isPlaceholder() { return placeholderCreated; }
    public void cleanup() {
        if (placeholderCreated && workingFlowFile != null) {
            try {
                session.remove(workingFlowFile);
            } finally {
                // Make idempotent
                //noinspection AssignmentToNull
                // (acceptable for lifecycle cleanup)
                // Prevent double-removal on repeated cleanup calls
                workingFlowFile = null;
            }
        }
    }

    // Property getters
    public String getBulkTransferDataFormat() { return bulkTransferDataFormat; }
    public String getTargetTable() { return targetTable; }
    public String getUpsertSqlTemplate() { return upsertSqlTemplate; }
    public boolean getUpsertReturnsRecords() {
        if (upsertReturnsRecords != null) return upsertReturnsRecords.booleanValue();
        // Fallback to descriptor default when not set
        final String def = UPSERT_RETURNS_RECORDS.getDefaultValue();
        return def != null && Boolean.parseBoolean(def);
    }

    public String getSourceTable() { return sourceTable; }
    public String getCustomQuery() { return customQuery; }
    public Integer getMaxRowsPerFlowFile() { return maxRowsPerFlowFile; }

    public String getMaximumValueColumns() { return maximumValueColumns; }
    public List<String> getMaximumValueColumnsList() {
        final List<String> list = new ArrayList<>();
        if (maximumValueColumns != null && !maximumValueColumns.isBlank()) {
            for (String c : maximumValueColumns.split(",")) {
                final String col = c.trim();
                if (!col.isEmpty()) list.add(col);
            }
        }
        return list;
    }

    public String getCsvTargetColumns() { return csvTargetColumns; }
    public String getTargetColumns() { return targetColumns; }
    public String getCsvDelimiter() { return csvDelimiter; }
    public String getCsvQuote() { return csvQuote; }
    public String getCsvEscape() { return csvEscape; }
    public String getCsvNullToken() { return csvNullToken; }

    public String getCsvHeader() { return csvHeader; }

    public String getParquetMatchBy() { 
        if (parquetMatchBy != null) return parquetMatchBy;
        final String def = PARQUET_MATCH_BY.getDefaultValue();
        return def != null ? def : "position";
    }
    public String getParquetVersion() { 
        if (parquetVersion != null) return parquetVersion;
        final String def = PARQUET_VERSION.getDefaultValue();
        return def != null ? def : "v2";
    }
    

    public boolean isStreamIncomingFile() {
        if (streamIncomingFile != null) return streamIncomingFile.booleanValue();
        final String def = STREAM_INCOMING_FILE.getDefaultValue();
        return def != null && Boolean.parseBoolean(def);
    }


    public Map<String, String> getLastMaxValuesFromState() {
        try {
            final StateMap state = context.getStateManager().getState(Scope.CLUSTER);
            final Map<String, String> map = new HashMap<>();
            for (Map.Entry<String, String> e : state.toMap().entrySet()) {
                if (e.getKey().startsWith("maxvalue.")) {
                    map.put(e.getKey().substring("maxvalue.".length()), e.getValue());
                }
            }
            return map;
        } catch (IOException e) {
            throw new ProcessException("Failed to read processor state", e);
        }
    }


    // Common
    public static final PropertyDescriptor CONNECTION_PROVIDER = new PropertyDescriptor.Builder()
            .name("postgresql-connection-provider")
            .displayName("PostgreSQL Connection Provider")
            .description("Specifies the Controller Service providing PostgreSQL connections")
            .identifiesControllerService(PostgreSQLConnectionProviderService.class)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor BULK_TRANSFER_DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("data-format")
            .displayName("Data Format")
            .description("Select CSV (use Record Reader/Writer) or Parquet (stream bytes via pg_parquet)")
            .allowableValues("CSV", "Parquet")
            .defaultValue("Parquet")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor STREAM_INCOMING_FILE = new PropertyDescriptor.Builder()
            .name("stream-incoming-file")
            .displayName("Stream Incoming File")
            .description("When true, stream the incoming FlowFile bytes directly to PostgreSQL COPY as-is according to Data Format. When false and Data Format is Parquet, a Parquet RecordSetWriter will be required to transcode from the configured Record Reader.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    // Load

    public static final PropertyDescriptor TARGET_COLUMNS = new PropertyDescriptor.Builder()
            .name("target-columns")
            .displayName("Target Columns")
            .description("Optional comma-separated list of target table columns to load. When set, the COPY statement will specify this ordered column list and records will be reordered accordingly. Missing fields will be written as NULL.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "CSV")
            .build();

    // Upsert
    public static final PropertyDescriptor TARGET_TABLE = new PropertyDescriptor.Builder()
        .name("target-table")
        .displayName("Target Table")
        .description("The fully-qualified name of the target table for upsert (optionally schema-qualified)")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();
    public static final PropertyDescriptor UPSERT_SQL_TEMPLATE = new PropertyDescriptor.Builder()
            .name("upsert-sql-template")
            .displayName("Upsert SQL Template")
            .description("Template SQL for upsert using placeholders ${temp_table} and ${target_table}. Example: \nINSERT INTO ${target_table} \nSELECT * FROM ${temp_table} \nON CONFLICT (tenant_id, key) DO UPDATE SET details = tgt.details || EXCLUDED.details, last_operation = EXCLUDED.last_operation, load_dt = EXCLUDED.load_dt \nRETURNING *")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("INSERT INTO ${target_table} \nSELECT * FROM ${temp_table} \nON CONFLICT (tenant_id, key) DO UPDATE SET details = tgt.details || EXCLUDED.details, last_operation = EXCLUDED.last_operation, load_dt = EXCLUDED.load_dt \nRETURNING *")
            .build();

            public static final PropertyDescriptor UPSERT_RETURNS_RECORDS = new PropertyDescriptor.Builder()
            .name("upsert-returns-records")
            .displayName("Upsert Returns Records")
            .description("Whether the upsert should return records. If true, these rows will be written to the output FlowFile.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue("true")
            .build();
    // Export 
    public static final PropertyDescriptor SOURCE_TABLE = new PropertyDescriptor.Builder()
            .name("source-table")
            .displayName("Source Table")
            .description("The schema-qualified table name to export when Custom Query is not specified.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor CUSTOM_QUERY = new PropertyDescriptor.Builder()
            .name("custom-query")
            .displayName("Custom Query")
            .description("Optional SQL to export using COPY (query) TO STDOUT. When set, Table Name is ignored. Incremental predicates and ordering will be applied by wrapping this query.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("max-rows-per-flow-file")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of rows to include in each FlowFile. Set 0 for no limit.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("0")
            .required(true)
            .build();
    public static final PropertyDescriptor MAXIMUM_VALUE_COLUMNS = new PropertyDescriptor.Builder()
            .name("maximum-value-columns")
            .displayName("Maximum-value Columns")
            .description("A comma-separated list of column names to use for maintaining state for incremental fetching. Supports multiple columns.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    // Record Reader / Writer
    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("FlowFile Record Reader")
            .description("Record Reader for parsing incoming FlowFiles (e.g., JSON, Avro, CSV, Parquet). When Data Format is Parquet and the Reader is Parquet, the processor will bypass reading and stream the Parquet content.")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .dependsOn(STREAM_INCOMING_FILE, "false")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("FlowFile Record Writer")
            .description("Record Writer for serializing outgoing FlowFile records (e.g., JSON, Avro, CSV, Parquet). When Data Format is Parquet and the Writer is Parquet, the processor will bypass writing and stream Parquet bytes directly.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();


    public static final PropertyDescriptor CSV_TARGET_COLUMNS = new PropertyDescriptor.Builder()
            .name("target-columns")
            .displayName("Target Columns")
            .description("Optional comma-separated list of target table columns to load. When set, the COPY statement will specify this ordered column list and records will be reordered accordingly. Missing fields will be written as NULL.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "CSV")
            .build();

    public static final PropertyDescriptor CSV_DELIMITER = new PropertyDescriptor.Builder()
            .name("csv-delimiter")
            .displayName("CSV Delimiter")
            .description("CSV delimiter for COPY")
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "CSV")
            .build();

    public static final PropertyDescriptor CSV_QUOTE = new PropertyDescriptor.Builder()
            .name("csv-quote")
            .displayName("CSV Quote Character")
            .description("CSV quote character for COPY")
            .defaultValue("\"")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "CSV")
            .build();

    public static final PropertyDescriptor CSV_ESCAPE = new PropertyDescriptor.Builder()
            .name("csv-escape")
            .displayName("CSV Escape Character")
            .description("CSV escape character for COPY")
            .defaultValue("\"")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "CSV")
            .build();

    public static final PropertyDescriptor CSV_NULL = new PropertyDescriptor.Builder()
            .name("csv-null")
            .displayName("CSV NULL Token")
            .description("String token representing NULL values in CSV for COPY")
            .defaultValue("\\N")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "CSV")
            .build();

    public static final PropertyDescriptor CSV_HEADER = new PropertyDescriptor.Builder()
            .name("csv-header")
            .displayName("CSV Header")
            .description("Specifies that the CSV file contains a header line with column names. " +
                    "When true, the first line is discarded during COPY FROM operations. " +
                    "When false, all lines are treated as data.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "CSV")
            .build();

    // Parquet
    public static final PropertyDescriptor PARQUET_MATCH_BY = new PropertyDescriptor.Builder()
            .name("parquet-match-by")
            .displayName("Parquet Match By")
            .description("Method to match Parquet fields to table columns when reading: position or name")
            .allowableValues("position", "name")
            .defaultValue("position")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "Parquet")
            .build();

    public static final PropertyDescriptor PARQUET_VERSION = new PropertyDescriptor.Builder()
            .name("parquet-version")
            .displayName("Parquet Writer Version")
            .description("Writer version: v1 or v2. You will need to use a vectorized reader to read v2 files.")
            .allowableValues("v1", "v2")
            .defaultValue("v2")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(BULK_TRANSFER_DATA_FORMAT, "Parquet")
            .build();

}


