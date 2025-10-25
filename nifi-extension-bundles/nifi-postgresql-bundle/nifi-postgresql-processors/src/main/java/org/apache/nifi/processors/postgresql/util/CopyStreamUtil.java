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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import org.postgresql.copy.CopyManager;

/**
 * Utility for PostgreSQL COPY operations using the CopyManager API. Provides true streaming support without loading data into memory. Follows the
 * official PostgreSQL JDBC documentation patterns.
 */
public final class CopyStreamUtil {

    private CopyStreamUtil() {
    }

    /**
     * Execute a COPY IN operation with an InputStream. This provides true streaming without loading data into memory.
     *
     * @param copyManager
     *            The PostgreSQL CopyManager instance
     * @param copySql
     *            The COPY SQL statement
     * @param inputStream
     *            The input stream containing the data
     * @return Number of rows affected
     * @throws SQLException
     *             If the COPY operation fails
     * @throws IOException
     *             If there's an I/O error
     */
    public static long executeCopyIn(final CopyManager copyManager, final String copySql, final InputStream inputStream)
            throws SQLException, IOException {
        return copyManager.copyIn(copySql, inputStream);
    }

    /**
     * Execute a COPY OUT operation writing to an OutputStream. This provides true streaming without loading data into memory.
     *
     * @param copyManager
     *            The PostgreSQL CopyManager instance
     * @param copySql
     *            The COPY SQL statement
     * @param outputStream
     *            The output stream to write the data to
     * @return Number of rows affected
     * @throws SQLException
     *             If the COPY operation fails
     * @throws IOException
     *             If there's an I/O error
     */
    public static long executeCopyOut(final CopyManager copyManager, final String copySql, final OutputStream outputStream)
            throws SQLException, IOException {
        return copyManager.copyOut(copySql, outputStream);
    }

    /**
     * Execute a COPY IN operation using a data writer function. This allows streaming data generation without loading everything into memory.
     *
     * @param copyManager
     *            The PostgreSQL CopyManager instance
     * @param copySql
     *            The COPY SQL statement
     * @param dataWriter
     *            Function that writes data to the provided OutputStream
     * @return Number of rows affected by the COPY operation
     * @throws SQLException
     *             If the COPY operation fails
     * @throws IOException
     *             If there's an I/O error
     */
    public static long executeCopyInWithWriter(final CopyManager copyManager, final String copySql, final DataWriter dataWriter)
            throws SQLException, IOException {
        final PipedOutputStream pipedOut = new PipedOutputStream();
        final PipedInputStream pipedIn = new PipedInputStream(pipedOut, 1024 * 1024); // 1MB buffer
        final AtomicReference<Throwable> writerError = new AtomicReference<>();
        final AtomicReference<Long> copyResult = new AtomicReference<>();
        final AtomicReference<Throwable> copyError = new AtomicReference<>();

        // Writer thread - generates data and writes to pipe
        final Thread writerThread = new Thread(() -> {
            try {
                dataWriter.writeData(pipedOut);
            } catch (Exception e) {
                writerError.set(e);
            } finally {
                try {
                    pipedOut.close();
                } catch (IOException ignored) {
                    // IOException ignored
                }
            }
        }, "nifi-pg-copy-writer");

        // Copy thread - reads from pipe and sends to PostgreSQL
        final Thread copyThread = new Thread(() -> {
            try {
                long result = copyManager.copyIn(copySql, pipedIn);
                copyResult.set(result);
            } catch (Exception e) {
                copyError.set(e);
            } finally {
                try {
                    pipedIn.close();
                } catch (IOException ignored) {
                    // IOException ignored
                }
            }
        }, "nifi-pg-copy-reader");

        // Start both threads
        writerThread.start();
        copyThread.start();

        // Wait for completion
        try {
            writerThread.join();
            copyThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("COPY operation interrupted", e);
        }

        // Check for errors
        final Throwable wError = writerError.get();
        final Throwable cError = copyError.get();

        if (wError != null) {
            if (wError instanceof IOException)
                throw (IOException) wError;
            if (wError instanceof SQLException)
                throw (SQLException) wError;
            throw new IOException("Writer error during COPY IN", wError);
        }

        if (cError != null) {
            if (cError instanceof IOException)
                throw (IOException) cError;
            if (cError instanceof SQLException)
                throw (SQLException) cError;
            throw new IOException("COPY error during COPY IN", cError);
        }

        final Long result = copyResult.get();
        return result != null ? result : 0L;
    }

    /**
     * Functional interface for writing data to an OutputStream during COPY IN operations.
     */
    @FunctionalInterface
    public interface DataWriter {
        void writeData(OutputStream outputStream) throws Exception;
    }

    // Temporary compatibility classes for PostgreSQLBulkExport until it's
    // refactored
    public static final class CopyOutContext {
        private final PipedInputStream pipedInput;
        private final PipedOutputStream pipedOutput;
        private final Thread worker;
        private final AtomicReference<Throwable> error;

        private CopyOutContext(final PipedInputStream pipedInput, final PipedOutputStream pipedOutput, final Thread worker,
                final AtomicReference<Throwable> error) {
            this.pipedInput = pipedInput;
            this.pipedOutput = pipedOutput;
            this.worker = worker;
            this.error = error;
        }

        public InputStream getInputStream() {
            return pipedInput;
        }

        public void joinAndRethrowIfError() {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            final Throwable t = error.get();
            if (t != null) {
                if (t instanceof RuntimeException re)
                    throw re;
                throw new RuntimeException(t);
            }
        }

        public void closeQuietly() {
            try {
                pipedOutput.close();
            } catch (IOException ignored) {
                // IOException ignored
            }
            try {
                pipedInput.close();
            } catch (IOException ignored) {
                // IOException ignored
            }
        }
    }

    public static CopyOutContext startCopyOut(final CopyManager copyManager, final String copySql, final String threadName) throws IOException {
        final PipedOutputStream pipedOut = new PipedOutputStream();
        final PipedInputStream pipedIn = new PipedInputStream(pipedOut, 1 << 16);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Thread worker = new Thread(() -> {
            try {
                copyManager.copyOut(copySql, pipedOut);
            } catch (SQLException | IOException e) {
                error.set(e);
            } finally {
                try {
                    pipedOut.close();
                } catch (IOException ignored) {
                    // IOException ignored
                }
            }
        }, threadName);
        worker.start();
        return new CopyOutContext(pipedIn, pipedOut, worker, error);
    }
}
