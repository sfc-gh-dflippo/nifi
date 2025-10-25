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

package org.apache.nifi.processors.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for PostgreSQLBulkLoad processor. Tests processor configuration, validation, and basic functionality without database connections.
 */
public class PostgreSQLBulkLoadTest {

    private TestRunner testRunner;
    private PostgreSQLBulkLoad processor;

    @BeforeEach
    public void setup() {
        processor = new PostgreSQLBulkLoad();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProcessorAnnotations() {
        // Verify processor has proper NiFi annotations
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.documentation.Tags.class));
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.documentation.CapabilityDescription.class));
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.InputRequirement.class));
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.SupportsBatching.class));

        // Verify tags contain expected values
        String[] tags = processor.getClass().getAnnotation(org.apache.nifi.annotation.documentation.Tags.class).value();
        assertTrue(java.util.Arrays.asList(tags).contains("postgresql"));
        assertTrue(java.util.Arrays.asList(tags).contains("bulk"));
        assertTrue(java.util.Arrays.asList(tags).contains("load"));
    }

    @Test
    public void testProcessorRelationships() {
        Set<Relationship> relationships = processor.getRelationships();

        // Verify processor has exactly 2 relationships
        assertEquals(2, relationships.size(), "Should have exactly 2 relationships (success and failure)");

        // Verify specific relationships exist
        assertTrue(relationships.contains(PostgreSQLBulkLoad.REL_SUCCESS));
        assertTrue(relationships.contains(PostgreSQLBulkLoad.REL_FAILURE));

        // Verify relationship names
        assertEquals("success", PostgreSQLBulkLoad.REL_SUCCESS.getName());
        assertEquals("failure", PostgreSQLBulkLoad.REL_FAILURE.getName());
    }

    @Test
    public void testProcessorProperties() {
        Collection<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();

        // Verify we have the expected number of properties
        assertTrue(properties.size() >= 8, "Should have at least 8 properties");

        // Verify required properties exist
        assertTrue(properties.contains(PostgreSQLBulkLoad.CONNECTION_PROVIDER));
        assertTrue(properties.contains(PostgreSQLBulkLoad.TARGET_TABLE));
        assertTrue(properties.contains(PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT));
        assertTrue(properties.contains(PostgreSQLBulkLoad.STREAM_INCOMING_FILE));
        assertTrue(properties.contains(PostgreSQLBulkLoad.RECORD_READER));
        assertTrue(properties.contains(PostgreSQLBulkLoad.RECORD_WRITER));

        // Verify property characteristics
        assertTrue(PostgreSQLBulkLoad.CONNECTION_PROVIDER.isRequired());
        assertTrue(PostgreSQLBulkLoad.TARGET_TABLE.isRequired());
        assertTrue(PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT.isRequired());

        // Verify default values
        assertEquals("Parquet", PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT.getDefaultValue());
        assertEquals("true", PostgreSQLBulkLoad.STREAM_INCOMING_FILE.getDefaultValue());
    }

    @Test
    public void testProcessorValidationWithoutConnectionProvider() {
        // Test that processor is invalid without connection provider
        Collection<ValidationResult> results = testRunner.validate();
        assertFalse(results.isEmpty(), "Should have validation errors without connection provider");

        boolean hasConnectionProviderError = results.stream().anyMatch(result -> result.getSubject().equals("PostgreSQL Connection Provider"));
        assertTrue(hasConnectionProviderError, "Should have connection provider validation error");
    }

    @Test
    public void testProcessorValidationWithoutTargetTable() {
        // Add a mock connection provider but no target table
        testRunner.setProperty(PostgreSQLBulkLoad.CONNECTION_PROVIDER, "mock-connection-provider");

        Collection<ValidationResult> results = testRunner.validate();
        assertFalse(results.isEmpty(), "Should have validation errors without target table");

        boolean hasTargetTableError = results.stream().anyMatch(result -> result.getSubject().equals("Target Table"));
        assertTrue(hasTargetTableError, "Should have target table validation error");
    }

    @Test
    public void testProcessorValidationWithRequiredProperties() {
        // Set required properties
        testRunner.setProperty(PostgreSQLBulkLoad.CONNECTION_PROVIDER, "mock-connection-provider");
        testRunner.setProperty(PostgreSQLBulkLoad.TARGET_TABLE, "test_table");
        testRunner.setProperty(PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT, "CSV");

        // Should still be invalid due to missing controller services, but no property
        // validation errors
        Collection<ValidationResult> results = testRunner.validate();

        // Check that basic property validation passes (controller service validation
        // may still fail)
        boolean hasBasicPropertyErrors = results.stream()
                .anyMatch(result -> result.getSubject().equals("Target Table") || result.getSubject().equals("Data Format"));
        assertFalse(hasBasicPropertyErrors, "Should not have basic property validation errors");
    }

    @Test
    public void testDataFormatOptions() {
        PropertyDescriptor dataFormat = PostgreSQLBulkLoad.BULK_TRANSFER_DATA_FORMAT;

        // Test valid values
        assertTrue(dataFormat.getAllowableValues().stream().anyMatch(av -> "CSV".equals(av.getValue())));
        assertTrue(dataFormat.getAllowableValues().stream().anyMatch(av -> "Parquet".equals(av.getValue())));

        // Verify default
        assertEquals("Parquet", dataFormat.getDefaultValue());
    }

    @Test
    public void testStreamIncomingFileProperty() {
        PropertyDescriptor streamProperty = PostgreSQLBulkLoad.STREAM_INCOMING_FILE;

        // Test valid values
        assertTrue(streamProperty.getAllowableValues().stream().anyMatch(av -> "true".equals(av.getValue())));
        assertTrue(streamProperty.getAllowableValues().stream().anyMatch(av -> "false".equals(av.getValue())));

        // Verify default
        assertEquals("true", streamProperty.getDefaultValue());
    }

    @Test
    public void testCSVFormatProperties() {
        Collection<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();

        // Verify CSV-specific properties exist
        assertTrue(properties.contains(PostgreSQLBulkLoad.CSV_DELIMITER));
        assertTrue(properties.contains(PostgreSQLBulkLoad.CSV_QUOTE));
        assertTrue(properties.contains(PostgreSQLBulkLoad.CSV_ESCAPE));
        assertTrue(properties.contains(PostgreSQLBulkLoad.CSV_NULL));

        // Verify default values
        assertEquals(",", PostgreSQLBulkLoad.CSV_DELIMITER.getDefaultValue());
        assertEquals("\"", PostgreSQLBulkLoad.CSV_QUOTE.getDefaultValue());
        assertEquals("\"", PostgreSQLBulkLoad.CSV_ESCAPE.getDefaultValue());
        assertEquals("\\N", PostgreSQLBulkLoad.CSV_NULL.getDefaultValue());
    }

    @Test
    public void testParquetProperties() {
        Collection<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();

        // Verify Parquet-specific properties exist
        assertTrue(properties.contains(PostgreSQLBulkLoad.PARQUET_MATCH_BY));

        // Verify default values
        assertEquals("position", PostgreSQLBulkLoad.PARQUET_MATCH_BY.getDefaultValue());
    }

    @Test
    public void testTargetColumnsProperty() {
        Collection<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();

        // Verify target columns property exists
        assertTrue(properties.contains(PostgreSQLBulkLoad.TARGET_COLUMNS));

        // Verify it's not required (optional)
        assertFalse(PostgreSQLBulkLoad.TARGET_COLUMNS.isRequired());

        // Verify it supports expression language
        assertTrue(PostgreSQLBulkLoad.TARGET_COLUMNS.isExpressionLanguageSupported());
    }

    @Test
    public void testProcessorInputRequirement() {
        // Verify processor requires input
        org.apache.nifi.annotation.behavior.InputRequirement inputReq = processor.getClass()
                .getAnnotation(org.apache.nifi.annotation.behavior.InputRequirement.class);
        assertNotNull(inputReq);
        assertEquals(org.apache.nifi.annotation.behavior.InputRequirement.Requirement.INPUT_REQUIRED, inputReq.value());
    }

    @Test
    public void testProcessorSupportsBatching() {
        // Verify processor supports batching
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.SupportsBatching.class));
    }

    @Test
    public void testProcessorWritesAttributes() {
        // Verify processor writes attributes
        org.apache.nifi.annotation.behavior.WritesAttributes writesAttrs = processor.getClass()
                .getAnnotation(org.apache.nifi.annotation.behavior.WritesAttributes.class);
        assertNotNull(writesAttrs);

        // Should write record.count attribute
        boolean hasRecordCountAttr = java.util.Arrays.stream(writesAttrs.value()).anyMatch(attr -> "record.count".equals(attr.attribute()));
        assertTrue(hasRecordCountAttr, "Should write record.count attribute");
    }
}
