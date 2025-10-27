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

import org.apache.nifi.processors.postgresql.util.TableMetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for TableMetadataCache class
 */
public class TableMetadataCacheTest {

    private TableMetadataCache cache;

    @BeforeEach
    public void setup() {
        cache = TableMetadataCache.getInstance();
        cache.clear(); // Start with clean cache
    }

    @AfterEach
    public void cleanup() {
        cache.clear(); // Clean up after each test
    }

    @Test
    public void testSingleton() {
        TableMetadataCache instance1 = TableMetadataCache.getInstance();
        TableMetadataCache instance2 = TableMetadataCache.getInstance();
        assertSame(instance1, instance2, "Should return same singleton instance");
    }

    @Test
    public void testPutAndGet() {
        TableMetadata metadata = createTestMetadata("mydb", "public", "users");

        cache.put("mydb", "public", "users", metadata);

        TableMetadata retrieved = cache.get("mydb", "public", "users", 0);
        assertNotNull(retrieved);
        assertEquals(3, retrieved.getAllColumns().size());
    }

    @Test
    public void testGetNonExistent() {
        TableMetadata retrieved = cache.get("mydb", "public", "nonexistent", 0);
        assertNull(retrieved, "Should return null for non-existent entry");
    }

    @Test
    public void testInvalidate() {
        TableMetadata metadata = createTestMetadata("mydb", "public", "users");
        cache.put("mydb", "public", "users", metadata);

        // Verify it's cached
        assertNotNull(cache.get("mydb", "public", "users", 0));

        // Invalidate
        cache.invalidate("mydb", "public", "users");

        // Should no longer be cached
        assertNull(cache.get("mydb", "public", "users", 0));
    }

    @Test
    public void testClear() {
        cache.put("mydb", "public", "users", createTestMetadata("mydb", "public", "users"));
        cache.put("mydb", "public", "orders", createTestMetadata("mydb", "public", "orders"));
        cache.put("otherdb", "public", "products", createTestMetadata("otherdb", "public", "products"));

        // Verify all are cached
        assertNotNull(cache.get("mydb", "public", "users", 0));
        assertNotNull(cache.get("mydb", "public", "orders", 0));
        assertNotNull(cache.get("otherdb", "public", "products", 0));

        // Clear all
        cache.clear();

        // All should be gone
        assertNull(cache.get("mydb", "public", "users", 0));
        assertNull(cache.get("mydb", "public", "orders", 0));
        assertNull(cache.get("otherdb", "public", "products", 0));
    }

    @Test
    public void testTTLExpiration() throws InterruptedException {
        TableMetadata metadata = createTestMetadata("mydb", "public", "users");
        cache.put("mydb", "public", "users", metadata);

        // Should be retrievable with no TTL
        assertNotNull(cache.get("mydb", "public", "users", 0));

        // Should be retrievable within TTL
        assertNotNull(cache.get("mydb", "public", "users", 5000));

        // Wait a bit and test with very short TTL
        Thread.sleep(100);
        assertNull(cache.get("mydb", "public", "users", 50), "Should be expired with short TTL");
    }

    @Test
    public void testNoTTLMeansNoExpiration() throws InterruptedException {
        TableMetadata metadata = createTestMetadata("mydb", "public", "users");
        cache.put("mydb", "public", "users", metadata);

        Thread.sleep(100);

        // With TTL=0, should never expire
        assertNotNull(cache.get("mydb", "public", "users", 0));
    }

    @Test
    public void testCacheKeyIsCaseSensitive() {
        TableMetadata metadata = createTestMetadata("mydb", "public", "users");
        cache.put("mydb", "public", "users", metadata);

        // Different case should be different key
        assertNull(cache.get("mydb", "public", "Users", 0));
        assertNull(cache.get("mydb", "Public", "users", 0));
        assertNull(cache.get("MyDb", "public", "users", 0));

        // Exact match should work
        assertNotNull(cache.get("mydb", "public", "users", 0));
    }

    @Test
    public void testMultipleDatabases() {
        cache.put("db1", "public", "users", createTestMetadata("db1", "public", "users"));
        cache.put("db2", "public", "users", createTestMetadata("db2", "public", "users"));

        TableMetadata db1Users = cache.get("db1", "public", "users", 0);
        TableMetadata db2Users = cache.get("db2", "public", "users", 0);

        assertNotNull(db1Users);
        assertNotNull(db2Users);
    }

    @Test
    public void testMultipleSchemas() {
        cache.put("mydb", "public", "users", createTestMetadata("mydb", "public", "users"));
        cache.put("mydb", "internal", "users", createTestMetadata("mydb", "internal", "users"));

        TableMetadata publicUsers = cache.get("mydb", "public", "users", 0);
        TableMetadata internalUsers = cache.get("mydb", "internal", "users", 0);

        assertNotNull(publicUsers);
        assertNotNull(internalUsers);
    }

    @Test
    public void testOverwriteExisting() {
        TableMetadata metadata1 = createTestMetadata("mydb", "public", "users");

        final Map<String, String> columnTypes2 = new HashMap<>();
        columnTypes2.put("id", "int4");
        columnTypes2.put("name", "varchar");
        columnTypes2.put("email", "varchar");
        columnTypes2.put("phone", "varchar");

        TableMetadata metadata2 = new TableMetadataImpl(
                "mydb",
                "public",
                "users",
                Arrays.asList("id", "name", "email", "phone"), // Different columns
                columnTypes2,
                new HashSet<>(Arrays.asList("id")),
                new HashSet<>()
        );

        cache.put("mydb", "public", "users", metadata1);
        TableMetadata retrieved1 = cache.get("mydb", "public", "users", 0);
        assertEquals(3, retrieved1.getAllColumns().size());

        // Overwrite with new metadata
        cache.put("mydb", "public", "users", metadata2);
        TableMetadata retrieved2 = cache.get("mydb", "public", "users", 0);
        assertEquals(4, retrieved2.getAllColumns().size());
    }

    private TableMetadata createTestMetadata(final String database, final String schema, final String table) {
        final Map<String, String> columnTypes = new HashMap<>();
        columnTypes.put("id", "int4");
        columnTypes.put("name", "varchar");
        columnTypes.put("email", "varchar");

        return new TableMetadataImpl(
                database,
                schema,
                table,
                Arrays.asList("id", "name", "email"),
                columnTypes,
                new HashSet<>(Arrays.asList("id")),
                new HashSet<>()
        );
    }
}

