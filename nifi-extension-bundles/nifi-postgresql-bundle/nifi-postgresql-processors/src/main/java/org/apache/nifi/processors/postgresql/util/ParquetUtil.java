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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public final class ParquetUtil {
    private ParquetUtil() {
    }

    // Row count helpers
    public static long countRows(final Path parquetFile) throws IOException {
        final InputFile inFile = new LocalInputFile(parquetFile);
        try (ParquetFileReader r = ParquetFileReader.open(inFile)) {
            final ParquetMetadata footer = r.getFooter();
            long rows = 0L;
            for (BlockMetaData b : footer.getBlocks())
                rows += b.getRowCount();
            return rows;
        }
    }

    public static long countRows(final InputFile inputFile) throws IOException {
        try (ParquetFileReader r = ParquetFileReader.open(inputFile)) {
            final ParquetMetadata footer = r.getFooter();
            long rows = 0L;
            for (BlockMetaData b : footer.getBlocks())
                rows += b.getRowCount();
            return rows;
        }
    }

    public static long countRows(final InputStream in) throws IOException {
        final Path temp = Files.createTempFile("nifi-parquet-count-", ".parquet");
        try {
            Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
            return countRows(temp);
        } finally {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
                // IOException ignored
            }
        }
    }

    // Splitting: emit FlowFiles directly
    public static List<FlowFile> splitParquetFileToFlowFiles(final ProcessSession session, final FlowFile base, final Path inputParquet,
            final int maxRowsPerFile, final ComponentLog logger) throws IOException {
        final List<FlowFile> outputs = new ArrayList<>();
        long totalRows = 0L;
        try {
            totalRows = countRows(inputParquet);
        } catch (Exception e) {
            logger.warn("Failed to read Parquet footer row count for {}: {}", inputParquet, e.getMessage(), e);
        }
        if (maxRowsPerFile <= 0 || (totalRows > 0 && totalRows <= maxRowsPerFile)) {
            FlowFile one = session.create(base);
            final Path p = inputParquet;
            final long rc = totalRows;
            one = session.write(one, out -> {
                try (InputStream fis = Files.newInputStream(p)) {
                    fis.transferTo(out);
                }
            });
            one = session.putAttribute(one, CoreAttributes.MIME_TYPE.key(), "application/parquet");
            if (rc > 0)
                one = session.putAttribute(one, "record.count", String.valueOf(rc));
            outputs.add(one);
            return outputs;
        }
        final InputFile inFile = new LocalInputFile(inputParquet);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inFile).build()) {
            GenericRecord rec = reader.read();
            if (rec == null)
                return outputs;
            final Schema avroSchema = rec.getSchema();
            while (rec != null) {
                final List<GenericRecord> chunk = new ArrayList<>(Math.max(1, maxRowsPerFile));
                int written = 0;
                do {
                    chunk.add(rec);
                    written++;
                    if (written >= maxRowsPerFile)
                        break;
                    rec = reader.read();
                } while (rec != null);

                final FlowFile[] holder = new FlowFile[]{session.create(base)};
                final int recordCount = chunk.size();
                holder[0] = session.write(holder[0], out -> {
                    OutputFile outFile = new OutputFile() {
                        @Override
                        public PositionOutputStream create(long blockSizeHint) {
                            return createOrOverwrite(blockSizeHint);
                        }
                        @Override
                        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                            return new FlowFilePositionOutputStream(out);
                        }
                        @Override
                        public String getPath() {
                            return "flowfile";
                        }
                        @Override
                        public boolean supportsBlockSize() {
                            return false;
                        }
                        @Override
                        public long defaultBlockSize() {
                            return 0L;
                        }
                    };
                    try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outFile)
                            .withSchema(avroSchema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            // Uncomment to force Parquet v1 format for maximum compatibility
                            //.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                            .build()) {
                        for (GenericRecord g : chunk)
                            writer.write(g);
                    }
                });
                holder[0] = session.putAttribute(holder[0], CoreAttributes.MIME_TYPE.key(), "application/parquet");
                holder[0] = session.putAttribute(holder[0], "record.count", String.valueOf(recordCount));
                outputs.add(holder[0]);
                rec = reader.read();
            }
        }
        return outputs;
    }

    // Local Parquet IO helpers
    static class LocalInputFile implements InputFile {
        private final Path path;
        LocalInputFile(final Path path) {
            this.path = path;
        }
        @Override
        public long getLength() throws IOException {
            return Files.size(path);
        }
        @Override
        public org.apache.parquet.io.SeekableInputStream newStream() throws IOException {
            return new LocalSeekableInputStream(path);
        }
    }

    static class LocalSeekableInputStream extends org.apache.parquet.io.SeekableInputStream {
        private final SeekableByteChannel channel;
        private final ByteBuffer single = ByteBuffer.allocate(1);
        LocalSeekableInputStream(final Path path) throws IOException {
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
            ByteBuffer buf = ByteBuffer.wrap(bytes, start, len);
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1)
                    throw new EOFException();
            }
        }
        @Override
        public int read(ByteBuffer buf) throws IOException {
            return channel.read(buf);
        }
        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1)
                    throw new EOFException();
            }
        }
        @Override
        public int read() throws IOException {
            single.clear();
            int r = channel.read(single);
            if (r == -1)
                return -1;
            single.flip();
            return single.get() & 0xFF;
        }
        @Override
        public int read(byte[] bytes, int off, int len) throws IOException {
            return channel.read(ByteBuffer.wrap(bytes, off, len));
        }
        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    static class FlowFilePositionOutputStream extends PositionOutputStream {
        private final OutputStream os;
        private long pos = 0;
        FlowFilePositionOutputStream(final OutputStream os) {
            this.os = os;
        }
        @Override
        public long getPos() {
            return pos;
        }
        @Override
        public void write(int b) throws IOException {
            os.write(b);
            pos++;
        }
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            os.write(b, off, len);
            pos += len;
        }
        @Override
        public void close() throws IOException {
            os.flush();
        }
    }
}
