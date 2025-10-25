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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for tracking and reporting performance timings in integration tests.
 */
public class PerformanceTimer {
    private final String testName;
    private final List<Timing> timings = new ArrayList<>();
    private long testStartTime;
    
    public PerformanceTimer(String testName) {
        this.testName = testName;
        this.testStartTime = System.currentTimeMillis();
    }
    
    public void start() {
        this.testStartTime = System.currentTimeMillis();
    }
    
    public TimingContext startOperation(String operationName) {
        return new TimingContext(operationName);
    }
    
    public void printSummary() {
        long totalTime = System.currentTimeMillis() - testStartTime;
        
        System.out.println("\n========================================");
        System.out.println("PERFORMANCE SUMMARY: " + testName);
        System.out.println("========================================");
        
        for (Timing timing : timings) {
            System.out.println(String.format("%-20s %6d ms (%6.2f sec)%s",
                timing.name + ":",
                timing.duration,
                timing.duration / 1000.0,
                timing.rowCount > 0 ? 
                    String.format(" - %8.0f rows/sec", timing.rowCount / (timing.duration / 1000.0)) : 
                    ""
            ));
        }
        
        System.out.println("----------------------------------------");
        System.out.println(String.format("%-20s %6d ms (%6.2f sec)",
            "Total:",
            totalTime,
            totalTime / 1000.0
        ));
        System.out.println("========================================\n");
    }
    
    public class TimingContext implements AutoCloseable {
        private final String name;
        private final long startTime;
        private int rowCount = 0;
        
        TimingContext(String name) {
            this.name = name;
            this.startTime = System.currentTimeMillis();
        }
        
        public void setRowCount(int count) {
            this.rowCount = count;
        }
        
        @Override
        public void close() {
            long duration = System.currentTimeMillis() - startTime;
            timings.add(new Timing(name, duration, rowCount));
        }
    }
    
    private static class Timing {
        final String name;
        final long duration;
        final int rowCount;
        
        Timing(String name, long duration, int rowCount) {
            this.name = name;
            this.duration = duration;
            this.rowCount = rowCount;
        }
    }
}

