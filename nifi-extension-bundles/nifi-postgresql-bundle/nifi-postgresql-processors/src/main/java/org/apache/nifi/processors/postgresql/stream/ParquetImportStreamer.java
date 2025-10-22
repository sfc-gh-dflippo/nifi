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
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** Imports Parquet into PostgreSQL via COPY FROM STDIN using Apache Parquet directly for PostgreSQL communication.
 * Still uses NiFi RecordReader for reading FlowFile input data. */
public final class ParquetImportStreamer {
    private ParquetImportStreamer() {}

    public static void streamParquetImport(final ProcessSession session,
                                           final FlowFile flowFile,
                                           final OutputStream out) {
        session.read(flowFile, in -> {
            final byte[] buffer = new byte[8192];
            int len;
            try {
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                out.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public static void transcodeToParquetImport(final ProcessSession session,
                                                final FlowFile flowFile,
                                                final RecordReaderFactory readerFactory,
                                                final OutputStream out,
                                                final ComponentLog logger) {
        // Use a temporary file to write Parquet data first, then stream to output
        // This avoids loading everything into memory while still providing proper Parquet format
        try {
            final Path tempFile = Files.createTempFile("nifi-pg-parquet-import-", ".parquet");
            
            try {
                session.read(flowFile, in -> {
                    try (RecordReader reader = readerFactory.createRecordReader(flowFile, in, logger)) {
                        final RecordSchema recordSchema = reader.getSchema();
                        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(recordSchema);
                        
                        // Create OutputFile for temporary file
                        final OutputFile outputFile = new LocalOutputFile(tempFile);
                        
                        // Stream records directly to Parquet file without loading all into memory
                        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                                .withSchema(avroSchema)
                                .withCompressionCodec(CompressionCodecName.SNAPPY)
                                .build()) {
                            
                            int recordCount = 0;
                            Record record;
                            while ((record = reader.nextRecord()) != null) {
                                // Convert NiFi Record to Avro GenericRecord one at a time
                                final Object avroObject = AvroTypeUtil.convertToAvroObject(record, avroSchema);
                                final GenericRecord avroRecord = (GenericRecord) avroObject;
                                writer.write(avroRecord);
                                recordCount++;
                            }
                            
                            logger.info("Transcoded {} records to Parquet format for PostgreSQL import", recordCount);
                        }
                    } catch (Exception e) {
                        throw new ProcessException("Failed to create Parquet data for COPY FROM", e);
                    }
                });
                
                // Now stream the Parquet file to output
                Files.copy(tempFile, out);
                
            } finally {
                // Clean up temporary file
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException e) {
                    logger.warn("Failed to delete temporary Parquet file: " + tempFile, e);
                }
            }
            
        } catch (IOException e) {
            throw new ProcessException("Failed to process temporary Parquet file", e);
        }
    }
    
    // Local OutputFile implementation for temporary files
    private static class LocalOutputFile implements OutputFile {
        private final Path path;
        
        LocalOutputFile(Path path) {
            this.path = path;
        }
        
        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return new LocalPositionOutputStream(Files.newOutputStream(path));
        }
        
        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return create(blockSizeHint);
        }
        
        @Override
        public boolean supportsBlockSize() {
            return false;
        }
        
        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }
    
    // Local PositionOutputStream implementation
    private static class LocalPositionOutputStream extends PositionOutputStream {
        private final OutputStream outputStream;
        private long position = 0;
        
        LocalPositionOutputStream(OutputStream outputStream) {
            this.outputStream = outputStream;
        }
        
        @Override
        public long getPos() throws IOException {
            return position;
        }
        
        @Override
        public void write(int b) throws IOException {
            outputStream.write(b);
            position++;
        }
        
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            outputStream.write(b, off, len);
            position += len;
        }
        
        @Override
        public void flush() throws IOException {
            outputStream.flush();
        }
        
        @Override
        public void close() throws IOException {
            outputStream.close();
        }
    }

}


