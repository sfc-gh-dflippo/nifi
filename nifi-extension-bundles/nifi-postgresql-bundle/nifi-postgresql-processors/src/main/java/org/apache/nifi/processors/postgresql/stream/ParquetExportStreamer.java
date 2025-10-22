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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.processors.postgresql.util.MaxValueTracker;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.hadoop.conf.Configuration;

/** Writes Parquet COPY TO results using NiFi Record writers with optional max-value tracking. */
public final class ParquetExportStreamer {
    private ParquetExportStreamer() {}

    public static FlowFile transcodeParquetExportToWriter(final ProcessSession session,
                                                          final FlowFile base,
                                                          final InputStream in,
                                                          final RecordSetWriterFactory writerFactory,
                                                          final ComponentLog logger,
                                                          final java.util.Map<String, String> attributesOut,
                                                          final MaxValueTracker maxTracker) {
        return session.write(base, out -> {
            // Create temporary file to store Parquet data from PostgreSQL
            Path tempFile = null;
            try {
                tempFile = Files.createTempFile("nifi-pg-parquet-export-", ".parquet");
                
                // Copy Parquet stream from PostgreSQL to temporary file
                Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
                
                // Create InputFile for reading the Parquet data
                final InputFile inputFile = new LocalInputFile(tempFile);
                
                // Read Parquet data and stream to NiFi records without loading all into memory
                Configuration conf = new Configuration();
                try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).withConf(conf).build()) {
                    
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
                        if (maxTracker != null) maxTracker.observe(nifiRecord);
                        writer.write(nifiRecord);
                        count++;
                        
                        // Stream remaining records one at a time
                        GenericRecord avroRecord;
                        while ((avroRecord = reader.read()) != null) {
                            valueMap = AvroTypeUtil.convertAvroRecordToMap(avroRecord, nifiSchema);
                            nifiRecord = new MapRecord(nifiSchema, valueMap);
                            if (maxTracker != null) maxTracker.observe(nifiRecord);
                            writer.write(nifiRecord);
                            count++;
                        }
                        
                        final WriteResult writeResult = writer.finishRecordSet();
                        attributesOut.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributesOut.put("record.count", String.valueOf(writeResult != null && writeResult.getRecordCount() > -1 ? writeResult.getRecordCount() : count));
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

    public static java.util.List<FlowFile> transcodeParquetExportToWriterChunked(final ProcessSession session,
                                                                                 final FlowFile base,
                                                                                 final InputStream in,
                                                                                 final RecordSetWriterFactory writerFactory,
                                                                                 final ComponentLog logger,
                                                                                 final int maxRows,
                                                                                 final MaxValueTracker maxTracker) {
        final java.util.List<FlowFile> outputs = new java.util.ArrayList<>();
        
        // Create temporary file to store Parquet data from PostgreSQL
        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("nifi-pg-parquet-export-chunked-", ".parquet");
            
            // Copy Parquet stream from PostgreSQL to temporary file
            Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
            
            // Create InputFile for reading the Parquet data
            final InputFile inputFile = new LocalInputFile(tempFile);
            
            // Read Parquet data and create chunked FlowFiles without loading all into memory
            Configuration conf = new Configuration();
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).withConf(conf).build()) {
                
                GenericRecord firstRecord = reader.read();
                if (firstRecord == null) {
                    return outputs;
                }
                
                final Schema avroSchema = firstRecord.getSchema();
                final org.apache.nifi.serialization.record.RecordSchema nifiSchema = AvroTypeUtil.createSchema(avroSchema);
                
                // Process records in chunks, streaming from Parquet reader
                GenericRecord currentRecord = firstRecord;
                while (currentRecord != null) {
                    final Map<String, String> attrs = new HashMap<>();
                    FlowFile part = session.create(base);
                    
                    // Collect records for this chunk before writing
                    final List<GenericRecord> chunkRecords = new ArrayList<>();
                    chunkRecords.add(currentRecord);
                    
                    // Read additional records for this chunk
                    for (int i = 1; i < maxRows; i++) {
                        GenericRecord nextRecord = reader.read();
                        if (nextRecord == null) break;
                        chunkRecords.add(nextRecord);
                    }
                    
                    part = session.write(part, out -> {
                        try (RecordSetWriter writer = writerFactory.createWriter(logger, nifiSchema, out, attrs)) {
                            writer.beginRecordSet();
                            
                            for (GenericRecord chunkRecord : chunkRecords) {
                                Map<String, Object> valueMap = AvroTypeUtil.convertAvroRecordToMap(chunkRecord, nifiSchema);
                                Record nifiRecord = new MapRecord(nifiSchema, valueMap);
                                if (maxTracker != null) maxTracker.observe(nifiRecord);
                                writer.write(nifiRecord);
                            }
                            
                            final WriteResult writeResult = writer.finishRecordSet();
                            attrs.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attrs.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        } catch (org.apache.nifi.schema.access.SchemaNotFoundException e) {
                            throw new IOException(e);
                        }
                    });
                    
                    part = session.putAllAttributes(part, attrs);
                    outputs.add(part);
                    
                    // Read next record for next chunk
                    currentRecord = reader.read();
                }
            }
            
        } catch (Exception e) {
            throw new ProcessException("Failed while chunking Parquet COPY TO results", e);
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
        return outputs;
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
        private final java.nio.channels.SeekableByteChannel channel;
        private final java.nio.ByteBuffer singleByte = java.nio.ByteBuffer.allocate(1);
        
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
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes, start, len);
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer);
                if (read == -1) {
                    throw new java.io.EOFException();
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
                    throw new java.io.EOFException();
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
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes, off, len);
            return channel.read(buffer);
        }
        
        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

}


