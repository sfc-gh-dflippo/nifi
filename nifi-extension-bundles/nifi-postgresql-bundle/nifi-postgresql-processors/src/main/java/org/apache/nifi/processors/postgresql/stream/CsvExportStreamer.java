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

package org.apache.nifi.processors.postgresql.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.postgresql.util.CsvFormats;
import org.apache.nifi.processors.postgresql.util.MaxValueTracker;
import org.apache.nifi.processors.postgresql.util.ProcessorProperties;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * Reads CSV exported via PostgreSQL COPY (query) TO STDOUT and writes records to FlowFiles using a RecordSetWriter.
 */
public final class CsvExportStreamer {
    private CsvExportStreamer() {
    }

    public static FlowFile writeCsvExportToFlowFile(final ProcessSession session, final FlowFile input, final ProcessorProperties properties,
            final RecordSetWriterFactory writerFactory, final InputStream in, final ComponentLog logger, final Map<String, String> attributesOut,
            final MaxValueTracker maxTracker) {
        final FlowFile[] holder = new FlowFile[]{input};
        holder[0] = session.write(holder[0], out -> {
            try {
                final CSVFormat csvFormat = CsvFormats.buildCsvParseFormat(properties);

                try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
                        CSVParser csvParser = new CSVParser(reader, csvFormat)) {

                    // Process records one by one for streaming
                    RecordSchema schema = null;
                    RecordSetWriter writer = null;
                    long count = 0L;

                    try {
                        for (CSVRecord csvRecord : csvParser) {
                            // Create schema from first record
                            if (schema == null) {
                                final List<RecordField> fields = new ArrayList<>();
                                for (int i = 0; i < csvRecord.size(); i++) {
                                    fields.add(new RecordField("column_" + i, RecordFieldType.STRING.getDataType()));
                                }
                                schema = new SimpleRecordSchema(fields);
                                writer = writerFactory.createWriter(logger, schema, out, attributesOut);
                                writer.beginRecordSet();
                            }

                            final Map<String, Object> values = new HashMap<>();
                            for (int i = 0; i < csvRecord.size(); i++) {
                                String value = csvRecord.get(i);
                                // Handle null values
                                if (value != null && value.equals(csvFormat.getNullString())) {
                                    value = null;
                                }
                                values.put("column_" + i, value);
                            }
                            final Record record = new MapRecord(schema, values);
                            if (maxTracker != null)
                                maxTracker.observe(record);
                            writer.write(record);
                            count++;
                        }

                        if (writer != null) {
                            final WriteResult writeResult = writer.finishRecordSet();
                            attributesOut.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attributesOut.put("record.count",
                                    String.valueOf(writeResult != null && writeResult.getRecordCount() > -1 ? writeResult.getRecordCount() : count));
                        } else {
                            // No records found
                            attributesOut.put(CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
                            attributesOut.put("record.count", "0");
                        }
                    } finally {
                        if (writer != null) {
                            writer.close();
                        }
                    }
                }
            } catch (SchemaNotFoundException | IOException e) {
                throw new ProcessException("Failed while reading/writing records", e);
            }
        });
        return holder[0];
    }

    /**
     * Reads CSV and writes multiple FlowFiles with up to maxRows rows each using the provided RecordSetWriterFactory. Returns the list of created
     * FlowFiles.
     */
    public static List<FlowFile> writeCsvExportChunked(final ProcessSession session, final FlowFile base, final ProcessorProperties properties,
            final RecordSetWriterFactory writerFactory, final InputStream in, final ComponentLog logger, final int maxRows,
            final MaxValueTracker maxTracker) {
        final List<FlowFile> outputs = new ArrayList<>();
        try {
            final CSVFormat csvFormat = CsvFormats.buildCsvParseFormat(properties);
            try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
                    CSVParser csvParser = new CSVParser(reader, csvFormat)) {

                // Read all records first to avoid lambda scope issues
                final List<CSVRecord> allRecords = csvParser.getRecords();
                if (allRecords.isEmpty()) {
                    return outputs;
                }

                // Create schema from first record
                final CSVRecord firstRecord = allRecords.get(0);
                final List<RecordField> fields = new ArrayList<>();
                for (int i = 0; i < firstRecord.size(); i++) {
                    fields.add(new RecordField("column_" + i, RecordFieldType.STRING.getDataType()));
                }
                final RecordSchema schema = new SimpleRecordSchema(fields);

                // Process records in chunks
                int recordIndex = 0;
                while (recordIndex < allRecords.size()) {
                    final Map<String, String> attrs = new HashMap<>();
                    FlowFile part = session.create(base);
                    final int startIndex = recordIndex;
                    final int endIndex = Math.min(startIndex + maxRows, allRecords.size());

                    part = session.write(part, out -> {
                        try (RecordSetWriter writer = writerFactory.createWriter(logger, schema, out, attrs)) {
                            writer.beginRecordSet();

                            for (int i = startIndex; i < endIndex; i++) {
                                final CSVRecord csvRecord = allRecords.get(i);
                                final Map<String, Object> values = new HashMap<>();
                                for (int j = 0; j < csvRecord.size(); j++) {
                                    String value = csvRecord.get(j);
                                    if (value != null && value.equals(csvFormat.getNullString())) {
                                        value = null;
                                    }
                                    values.put("column_" + j, value);
                                }
                                final Record record = new MapRecord(schema, values);
                                if (maxTracker != null)
                                    maxTracker.observe(record);
                                writer.write(record);
                            }

                            final WriteResult writeResult = writer.finishRecordSet();
                            attrs.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attrs.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        } catch (SchemaNotFoundException e) {
                            throw new IOException(e);
                        }
                    });

                    part = session.putAllAttributes(part, attrs);
                    outputs.add(part);
                    recordIndex = endIndex;
                }
            }
        } catch (IOException e) {
            throw new ProcessException("Failed while reading/writing records", e);
        }
        return outputs;
    }

    public static FlowFile writeCsvExportToFlowFileWithSchema(final ProcessSession session, final FlowFile input,
            final ProcessorProperties properties, final RecordSetWriterFactory writerFactory, final RecordSchema writeSchema, final InputStream in,
            final ComponentLog logger, final Map<String, String> attributesOut, final MaxValueTracker maxTracker) {
        final FlowFile[] holder = new FlowFile[]{input};
        holder[0] = session.write(holder[0], out -> {
            try {
                final CSVFormat csvFormat = CsvFormats.buildCsvParseFormat(properties);

                try (InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
                        CSVParser csvParser = new CSVParser(reader, csvFormat);
                        RecordSetWriter writer = writerFactory.createWriter(logger, writeSchema, out, attributesOut)) {

                    writer.beginRecordSet();
                    long count = 0L;
                    final List<RecordField> fields = writeSchema.getFields();

                    for (CSVRecord csvRecord : csvParser) {
                        final Map<String, Object> values = new HashMap<>();

                        // Map CSV columns to schema fields
                        for (int i = 0; i < Math.min(csvRecord.size(), fields.size()); i++) {
                            String value = csvRecord.get(i);
                            // Handle null values
                            if (value != null && value.equals(csvFormat.getNullString())) {
                                value = null;
                            }
                            values.put(fields.get(i).getFieldName(), value);
                        }

                        final Record record = new MapRecord(writeSchema, values);
                        if (maxTracker != null)
                            maxTracker.observe(record);
                        writer.write(record);
                        count++;
                    }

                    final WriteResult writeResult = writer.finishRecordSet();
                    attributesOut.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    attributesOut.put("record.count",
                            String.valueOf(writeResult != null && writeResult.getRecordCount() > -1 ? writeResult.getRecordCount() : count));
                }
            } catch (SchemaNotFoundException | IOException e) {
                throw new ProcessException("Failed while reading/writing records", e);
            }
        });
        return holder[0];
    }
}
