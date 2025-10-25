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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.postgresql.util.CsvFormats;
import org.apache.nifi.processors.postgresql.util.ProcessorProperties;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * Imports CSV into PostgreSQL via COPY FROM STDIN using Apache Commons CSV directly for PostgreSQL communication. Still uses NiFi RecordReader for
 * reading FlowFile input data.
 */
public final class CsvImportStreamer {
    private CsvImportStreamer() {
    }

    // Stream raw CSV bytes directly to COPY FROM
    public static void streamCsvImport(final ProcessSession session, final FlowFile flowFile, final OutputStream out) {
        session.read(flowFile, in -> {
            final byte[] buffer = new byte[8192];
            int len;
            try {
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                out.flush();
            } catch (IOException e) {
                throw new ProcessException("Failed while streaming CSV bytes to COPY FROM", e);
            }
        });
    }

    /**
     * Stream CSV data directly from NiFi records to an OutputStream for PostgreSQL COPY IN operation. This provides true streaming without loading
     * data into memory.
     *
     * @param session
     *            The ProcessSession
     * @param flowFile
     *            The FlowFile to read records from
     * @param readerFactory
     *            The RecordReaderFactory to create the reader
     * @param properties
     *            The processor properties
     * @param outputStream
     *            The output stream to write CSV data to
     * @param logger
     *            The component logger
     * @return Number of records processed
     */
    public static long streamCsvFromRecords(final ProcessSession session, final FlowFile flowFile, final RecordReaderFactory readerFactory,
            final ProcessorProperties properties, final OutputStream outputStream, final ComponentLog logger) {
        final long[] count = new long[]{0L};

        session.read(flowFile, in -> {
            try (RecordReader reader = readerFactory.createRecordReader(flowFile, in, logger)) {
                final RecordSchema inputSchema = reader.getSchema();
                final List<String> headerFields = resolveHeaderFields(properties, inputSchema);
                final CSVFormat csvFormat = CsvFormats.buildCsvPrintFormat(properties);

                try (OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
                        CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {

                    // Write header row first since COPY expects HEADER true
                    csvPrinter.printRecord(headerFields);

                    Record record;
                    while ((record = reader.nextRecord()) != null) {
                        final List<Object> values = new ArrayList<>(headerFields.size());
                        for (String fieldName : headerFields) {
                            Object value = record.getValue(fieldName);
                            // Convert null values to the configured null token
                            if (value == null) {
                                value = csvFormat.getNullString();
                            }
                            values.add(value);
                        }
                        csvPrinter.printRecord(values);
                        count[0]++;
                    }
                    csvPrinter.flush();
                }
            } catch (Exception ex) {
                throw new ProcessException("Failed while streaming CSV data to COPY IN", ex);
            }
        });

        return count[0];
    }

    private static List<String> resolveHeaderFields(final ProcessorProperties properties, final RecordSchema schema) {
        final String cols = properties.getCsvTargetColumns() != null ? properties.getCsvTargetColumns() : properties.getTargetColumns();
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
            if (!name.isEmpty())
                ordered.add(name);
        }
        return ordered;
    }
}
