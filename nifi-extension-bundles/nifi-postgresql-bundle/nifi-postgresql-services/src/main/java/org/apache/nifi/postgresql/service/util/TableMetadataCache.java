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

package org.apache.nifi.postgresql.service.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Thread-safe singleton cache for PostgreSQL table metadata. Provides TTL-based expiration and is shared across all PostgreSQLConnectionPool
 * instances.
 */
public class TableMetadataCache {
    private static final TableMetadataCache INSTANCE = new TableMetadataCache();
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private TableMetadataCache() {
        // Start TTL cleanup task
        scheduler.scheduleAtFixedRate(this::cleanupExpiredEntries, 1, 1, TimeUnit.MINUTES);
    }

    public static TableMetadataCache getInstance() {
        return INSTANCE;
    }

    /**
     * Retrieves cached table metadata if it exists and is not expired.
     *
     * @param database  the database name
     * @param schema    the schema name
     * @param table     the table name
     * @param ttlMillis time-to-live in milliseconds (0 = no expiration)
     * @return the cached TableMetadata, or null if not found or expired
     */
    public TableMetadata get(final String database, final String schema, final String table, final long ttlMillis) {
        final String key = buildKey(database, schema, table);
        CacheEntry entry = cache.get(key);

        if (entry != null && (ttlMillis == 0 || System.currentTimeMillis() < entry.timestamp + ttlMillis)) {
            return entry.metadata;
        }
        invalidate(database, schema, table); // Expired or invalid, remove it
        return null;
    }

    /**
     * Stores table metadata in the cache.
     *
     * @param database the database name
     * @param schema   the schema name
     * @param table    the table name
     * @param metadata the table metadata to cache
     */
    public void put(final String database, final String schema, final String table, final TableMetadata metadata) {
        final String key = buildKey(database, schema, table);
        cache.put(key, new CacheEntry(metadata, System.currentTimeMillis()));
    }

    /**
     * Invalidates a specific table's cached metadata.
     *
     * @param database the database name
     * @param schema   the schema name
     * @param table    the table name
     */
    public void invalidate(final String database, final String schema, final String table) {
        final String key = buildKey(database, schema, table);
        cache.remove(key);
    }

    /**
     * Clears all cached metadata.
     */
    public void clear() {
        cache.clear();
    }

    /**
     * Cleans up expired entries based on a default TTL. This is called periodically by the scheduler.
     */
    private void cleanupExpiredEntries() {
        // For now, we don't have a global TTL, so this is a no-op
        // Individual TTLs are checked on get()
    }

    private String buildKey(final String database, final String schema, final String table) {
        return database + "." + schema + "." + table;
    }

    private static class CacheEntry {
        final TableMetadata metadata;
        final long timestamp;

        CacheEntry(TableMetadata metadata, long timestamp) {
            this.metadata = metadata;
            this.timestamp = timestamp;
        }
    }

    /**
     * Shuts down the cache's background scheduler. Should be called on application shutdown.
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

