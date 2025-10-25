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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.postgresql.util.MaxValueTracker;
import org.apache.nifi.processors.postgresql.util.ParquetUtil;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * Writes Parquet COPY TO results using NiFi Record writers with optional max-value tracking.
 */
public final class ParquetExportStreamer {
    private ParquetExportStreamer() {
    }

    // Deprecated path (kept temporarily for compatibility). Use
    // writeParquetBytesToFlowFile instead.
    public static FlowFile transcodeParquetExportToWriter(final ProcessSession session, final FlowFile base, final InputStream in,
            final RecordSetWriterFactory writerFactory, final ComponentLog logger, final Map<String, String> attributesOut,
            final MaxValueTracker maxTracker) {
        return session.write(base, out -> {
            // Create temporary file to store Parquet data from PostgreSQL
            Path tempFile = null;
            try {
                tempFile = Files.createTempFile("nifi-pg-parquet-export-", ".parquet");

                // Copy Parquet stream from PostgreSQL to temporary file
                long bytesCopied = Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
                logger.debug("Copied {} bytes from PostgreSQL COPY TO to temporary Parquet file: {}", bytesCopied, tempFile);

                // Handle empty result set (0 bytes = no data from PostgreSQL)
                if (bytesCopied == 0) {
                    logger.debug("PostgreSQL COPY TO produced 0 bytes - likely empty result set");
                    attributesOut.put(CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
                    attributesOut.put("record.count", "0");
                    return;
                }

                // Create InputFile for reading the Parquet data
                final InputFile inputFile = new LocalInputFile(tempFile);

                // Read Parquet data and stream to NiFi records without loading all into memory
                // Note: No Hadoop Configuration needed - Parquet auto-discovers codecs via
                // ServiceLoader
                try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {

                    GenericRecord firstRecord = reader.read();
                    if (firstRecord == null) {
                        attributesOut.put(CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
                        attributesOut.put("record.count", "0");
                        return;
                    }

                    final Schema avroSchema = firstRecord.getSchema();
                    final org.apache.nifi.serialization.record.RecordSchema nifiSchema = AvroTypeUtil.createSchema(avroSchema);

                    try (RecordSetWriter writer = writerFactory.createWriter(logger, nifiSchema, out, attributesOut)) {
                        writer.beginRecordSet();
                        long count = 0L;

                        // Process first record
                        Map<String, Object> valueMap = AvroTypeUtil.convertAvroRecordToMap(firstRecord, nifiSchema);
                        Record nifiRecord = new MapRecord(nifiSchema, valueMap);
                        if (maxTracker != null)
                            maxTracker.observe(nifiRecord);
                        writer.write(nifiRecord);
                        count++;

                        // Stream remaining records one at a time
                        GenericRecord avroRecord;
                        while ((avroRecord = reader.read()) != null) {
                            valueMap = AvroTypeUtil.convertAvroRecordToMap(avroRecord, nifiSchema);
                            nifiRecord = new MapRecord(nifiSchema, valueMap);
                            if (maxTracker != null)
                                maxTracker.observe(nifiRecord);
                            writer.write(nifiRecord);
                            count++;
                        }

                        final WriteResult writeResult = writer.finishRecordSet();
                        attributesOut.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributesOut.put("record.count",
                                String.valueOf(writeResult != null && writeResult.getRecordCount() > -1 ? writeResult.getRecordCount() : count));
                    }
                }

            } catch (Exception e) {
                throw new ProcessException("Failed while reading Parquet data from PostgreSQL COPY TO", e);
            } finally {
                // Clean up temporary file
                if (tempFile != null) {
                    try {
                        Files.deleteIfExists(tempFile);
                    } catch (IOException e) {
                        logger.warn("Failed to delete temporary Parquet file: " + tempFile, e);
                    }
                }
            }
        });
    }

    public static FlowFile writeParquetBytesToFlowFile(final ProcessSession session, final FlowFile base, final InputStream in,
            final ComponentLog logger, final Map<String, String> attributesOut, final MaxValueTracker maxTracker) {
        FlowFile outFlow = base;
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("nifi-pg-parquet-export-raw-", ".parquet");
            long bytesCopied = Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Copied {} bytes from PostgreSQL COPY TO to temporary Parquet file: {}", bytesCopied, tempFile);
            if (bytesCopied == 0) {
                attributesOut.put(CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
                attributesOut.put("record.count", "0");
                return outFlow;
            }
            // Read row count from footer and stream bytes to FlowFile
            long rowCount;
            try {
                rowCount = ParquetUtil.countRows(tempFile);
            } catch (Exception e) {
                rowCount = -1L;
                logger.warn("Failed to read Parquet row count from footer: {}", e.getMessage(), e);
            }

            // Observe records for maxTracker if needed
            if (maxTracker != null) {
                try {
                    observeParquetRecords(tempFile, maxTracker, logger);
                } catch (IOException e) {
                    logger.warn("Failed to observe records for max value tracking: {}", e.getMessage(), e);
                }
            }

            final Path finalTemp = tempFile;
            final long rows = rowCount;
            outFlow = session.write(outFlow, out -> {
                try (InputStream fis = Files.newInputStream(finalTemp)) {
                    fis.transferTo(out);
                }
            });
            attributesOut.put(CoreAttributes.MIME_TYPE.key(), "application/parquet");
            if (rows >= 0)
                attributesOut.put("record.count", String.valueOf(rows));
        } catch (Exception e) {
            throw new ProcessException("Failed while writing Parquet bytes to FlowFile", e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                    // IOException ignored
                }
            }
        }
        return outFlow;
    }

    public static List<FlowFile> splitParquetBytesToFlowFiles(final ProcessSession session, final FlowFile base, final InputStream in,
            final ComponentLog logger, final int maxRows, final MaxValueTracker maxTracker) {
        final List<FlowFile> outputs = new ArrayList<>();
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("nifi-pg-parquet-export-split-", ".parquet");
            long bytesCopied = Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Copied {} bytes from PostgreSQL COPY TO to temporary Parquet file: {}", bytesCopied, tempFile);
            if (bytesCopied == 0)
                return outputs;

            // If maxTracker is set, we MUST observe all records for incremental fetching
            if (maxTracker != null) {
                // Observe all records first, regardless of file size
                try {
                    observeParquetRecords(tempFile, maxTracker, logger);
                } catch (Exception e) {
                    logger.warn("Failed to observe Parquet records for maxTracker: {}", e.getMessage(), e);
                }
            }

            // Now decide whether to split based on row count
            long rowCount = -1L;
            try {
                rowCount = ParquetUtil.countRows(tempFile);
            } catch (Exception e) {
                logger.warn("Failed to read Parquet row count: {}", e.getMessage(), e);
            }

            if (rowCount > 0 && rowCount <= maxRows) {
                // File is small enough - create single FlowFile
                FlowFile output = session.create(base);
                final Path finalTempFile = tempFile; // Make final for lambda
                output = session.write(output, out -> {
                    try {
                        Files.copy(finalTempFile, out);
                    } catch (IOException e) {
                        throw new ProcessException("Failed to write Parquet data to FlowFile", e);
                    }
                });
                output = session.putAttribute(output, CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
                output = session.putAttribute(output, "record.count", String.valueOf(rowCount));
                outputs.add(output);
            } else {
                // File is too large - split into multiple FlowFiles
                final List<FlowFile> parts = ParquetUtil.splitParquetFileToFlowFiles(session, base, tempFile, maxRows, logger);
                outputs.addAll(parts);
            }
        } catch (Exception e) {
            throw new ProcessException("Failed while splitting Parquet COPY TO results", e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                    // IOException ignored
                }
            }
        }
        return outputs;
    }

    /**
     * Observes all records in a Parquet file for max value tracking. This is necessary for incremental fetching when files are large and split.
     */
    private static void observeParquetRecords(final Path parquetFile, final MaxValueTracker maxTracker, final ComponentLog logger)
            throws IOException {
        final InputFile inputFile = new LocalInputFile(parquetFile);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            // Get schema from first record
            GenericRecord firstRecord = reader.read();
            if (firstRecord == null)
                return; // Empty file

            Schema avroSchema = firstRecord.getSchema();
            org.apache.nifi.serialization.record.RecordSchema nifiSchema = AvroTypeUtil.createSchema(avroSchema);

            // Observe first record
            Map<String, Object> valueMap = AvroTypeUtil.convertAvroRecordToMap(firstRecord, nifiSchema);
            Record nifiRecord = new MapRecord(nifiSchema, valueMap);
            maxTracker.observe(nifiRecord);

            // Observe remaining records
            GenericRecord avroRecord;
            while ((avroRecord = reader.read()) != null) {
                valueMap = AvroTypeUtil.convertAvroRecordToMap(avroRecord, nifiSchema);
                nifiRecord = new MapRecord(nifiSchema, valueMap);
                maxTracker.observe(nifiRecord);
            }

            logger.debug("Observed all records in Parquet file for maxTracker");
        }
    }

    // Local InputFile implementation for reading temporary files
    private static class LocalInputFile implements InputFile {
        private final Path path;

        LocalInputFile(Path path) {
            this.path = path;
        }

        @Override
        public long getLength() throws IOException {
            return Files.size(path);
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new LocalSeekableInputStream(path);
        }
    }

    // Local SeekableInputStream implementation
    private static class LocalSeekableInputStream extends SeekableInputStream {
        private final SeekableByteChannel channel;
        private final ByteBuffer singleByte = ByteBuffer.allocate(1);

        LocalSeekableInputStream(Path path) throws IOException {
            this.channel = Files.newByteChannel(path);
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
            channel.position(newPos);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(bytes, start, len);
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            return channel.read(buf);
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                int read = channel.read(buf);
                if (read == -1) {
                    throw new EOFException();
                }
            }
        }

        @Override
        public int read() throws IOException {
            singleByte.clear();
            int read = channel.read(singleByte);
            if (read == -1) {
                return -1;
            }
            singleByte.flip();
            return singleByte.get() & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
            ByteBuffer buffer = ByteBuffer.wrap(bytes, off, len);
            return channel.read(buffer);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

}
