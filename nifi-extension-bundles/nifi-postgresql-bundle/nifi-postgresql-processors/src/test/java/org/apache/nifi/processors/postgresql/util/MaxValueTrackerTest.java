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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for MaxValueTracker utility class.
 */
public class MaxValueTrackerTest {

    @Test
    public void testMaxValueTracking() {
        final MaxValueTracker tracker = new MaxValueTracker(Arrays.asList("id", "timestamp"));
        
        // Test with Map data
        Map<String, Object> record1 = new HashMap<>();
        record1.put("id", 1);
        record1.put("timestamp", "2023-01-01");
        record1.put("name", "Alice");
        tracker.observe(record1);
        
        Map<String, Object> record2 = new HashMap<>();
        record2.put("id", 3);
        record2.put("timestamp", "2023-01-02");
        record2.put("name", "Bob");
        tracker.observe(record2);
        
        Map<String, Object> record3 = new HashMap<>();
        record3.put("id", 2);
        record3.put("timestamp", "2023-01-03");
        record3.put("name", "Charlie");
        tracker.observe(record3);
        
        // Verify max values
        Map<String, String> maxValues = tracker.getMaxValuesAsStrings();
        Assertions.assertEquals("3", maxValues.get("id"));
        Assertions.assertEquals("2023-01-03", maxValues.get("timestamp"));
        Assertions.assertNull(maxValues.get("name")); // Not tracked
        
        // Test reset
        tracker.reset();
        maxValues = tracker.getMaxValuesAsStrings();
        Assertions.assertTrue(maxValues.isEmpty());
    }
}
