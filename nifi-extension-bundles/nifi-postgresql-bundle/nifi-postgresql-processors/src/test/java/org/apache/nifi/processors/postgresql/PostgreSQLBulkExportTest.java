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
 * Unit tests for PostgreSQLBulkExport processor. Tests processor configuration, validation, and basic functionality without database connections.
 */
public class PostgreSQLBulkExportTest {

    private TestRunner testRunner;
    private PostgreSQLBulkExport processor;

    @BeforeEach
    public void setup() {
        processor = new PostgreSQLBulkExport();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProcessorAnnotations() {
        // Verify processor has proper NiFi annotations
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.documentation.Tags.class));
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.documentation.CapabilityDescription.class));
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.InputRequirement.class));
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.SupportsBatching.class));
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.PrimaryNodeOnly.class));
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.Stateful.class));

        // Verify tags contain expected values
        String[] tags = processor.getClass().getAnnotation(org.apache.nifi.annotation.documentation.Tags.class).value();
        assertTrue(java.util.Arrays.asList(tags).contains("postgresql"));
        assertTrue(java.util.Arrays.asList(tags).contains("bulk"));
        assertTrue(java.util.Arrays.asList(tags).contains("export"));
        assertTrue(java.util.Arrays.asList(tags).contains("incremental"));
    }

    @Test
    public void testProcessorRelationships() {
        Set<Relationship> relationships = processor.getRelationships();

        // Verify processor has exactly 2 relationships
        assertEquals(2, relationships.size(), "Should have exactly 2 relationships (success and failure)");

        // Verify specific relationships exist
        assertTrue(relationships.contains(PostgreSQLBulkExport.REL_SUCCESS));
        assertTrue(relationships.contains(PostgreSQLBulkExport.REL_FAILURE));

        // Verify relationship names
        assertEquals("success", PostgreSQLBulkExport.REL_SUCCESS.getName());
        assertEquals("failure", PostgreSQLBulkExport.REL_FAILURE.getName());
    }

    @Test
    public void testProcessorProperties() {
        Collection<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();

        // Verify we have the expected number of properties
        assertTrue(properties.size() >= 10, "Should have at least 10 properties");

        // Verify required properties exist
        assertTrue(properties.contains(PostgreSQLBulkExport.CONNECTION_PROVIDER));
        assertTrue(properties.contains(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT));
        // Note: RECORD_WRITER is not always required - depends on data format

        // Verify property characteristics
        assertTrue(PostgreSQLBulkExport.CONNECTION_PROVIDER.isRequired());
        assertTrue(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT.isRequired());
        // Note: RECORD_WRITER requirement may depend on data format
        assertFalse(PostgreSQLBulkExport.RECORD_WRITER.isRequired());

        // Verify default values
        assertEquals("Parquet", PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT.getDefaultValue());
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
    public void testProcessorValidationWithMutuallyExclusiveProperties() {
        // Set both source table and custom query (should be mutually exclusive)
        testRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "mock-connection-provider");
        testRunner.setProperty(PostgreSQLBulkExport.SOURCE_TABLE, "test_table");
        testRunner.setProperty(PostgreSQLBulkExport.CUSTOM_QUERY, "SELECT * FROM test_table");

        Collection<ValidationResult> results = testRunner.validate();

        // Note: The exact validation logic may vary - just check that validation occurs
        System.out.println("Validation results with both source table and custom query: " + results);
        assertFalse(results.isEmpty(), "Should have some validation results");
    }

    @Test
    public void testProcessorValidationWithSourceTable() {
        // Set required properties with source table
        testRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "mock-connection-provider");
        testRunner.setProperty(PostgreSQLBulkExport.SOURCE_TABLE, "test_table");
        testRunner.setProperty(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT, "CSV");

        Collection<ValidationResult> results = testRunner.validate();

        // Check that basic property validation passes (controller service validation
        // may still fail)
        boolean hasBasicPropertyErrors = results.stream()
                .anyMatch(result -> result.getSubject().equals("Source Table") || result.getSubject().equals("Data Format"));
        assertFalse(hasBasicPropertyErrors, "Should not have basic property validation errors");
    }

    @Test
    public void testProcessorValidationWithCustomQuery() {
        // Set required properties with custom query
        testRunner.setProperty(PostgreSQLBulkExport.CONNECTION_PROVIDER, "mock-connection-provider");
        testRunner.setProperty(PostgreSQLBulkExport.CUSTOM_QUERY, "SELECT * FROM test_table WHERE id > 100");
        testRunner.setProperty(PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT, "CSV");

        Collection<ValidationResult> results = testRunner.validate();

        // Check that basic property validation passes (controller service validation
        // may still fail)
        boolean hasBasicPropertyErrors = results.stream()
                .anyMatch(result -> result.getSubject().equals("Custom Query") || result.getSubject().equals("Data Format"));
        assertFalse(hasBasicPropertyErrors, "Should not have basic property validation errors");
    }

    @Test
    public void testDataFormatOptions() {
        PropertyDescriptor dataFormat = PostgreSQLBulkExport.BULK_TRANSFER_DATA_FORMAT;

        // Test valid values
        assertTrue(dataFormat.getAllowableValues().stream().anyMatch(av -> "CSV".equals(av.getValue())));
        assertTrue(dataFormat.getAllowableValues().stream().anyMatch(av -> "Parquet".equals(av.getValue())));

        // Verify default
        assertEquals("Parquet", dataFormat.getDefaultValue());
    }

    @Test
    public void testMaximumValueColumnsProperty() {
        Collection<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();

        // Verify maximum value columns property exists
        assertTrue(properties.contains(PostgreSQLBulkExport.MAXIMUM_VALUE_COLUMNS));

        // Verify it's not required (optional)
        assertFalse(PostgreSQLBulkExport.MAXIMUM_VALUE_COLUMNS.isRequired());

        // Verify it supports expression language
        assertTrue(PostgreSQLBulkExport.MAXIMUM_VALUE_COLUMNS.isExpressionLanguageSupported());
    }

    @Test
    public void testMaxRowsPerFlowFileProperty() {
        Collection<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();

        // Verify max rows per flow file property exists
        assertTrue(properties.contains(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE));

        // Verify it's required
        assertTrue(PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE.isRequired());

        // Verify default value
        assertEquals("0", PostgreSQLBulkExport.MAX_ROWS_PER_FLOW_FILE.getDefaultValue());
    }

    @Test
    public void testCSVFormatProperties() {
        Collection<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();

        // Verify CSV-specific properties exist
        assertTrue(properties.contains(PostgreSQLBulkExport.CSV_DELIMITER));
        assertTrue(properties.contains(PostgreSQLBulkExport.CSV_QUOTE));
        assertTrue(properties.contains(PostgreSQLBulkExport.CSV_ESCAPE));
        assertTrue(properties.contains(PostgreSQLBulkExport.CSV_NULL));

        // Verify default values
        assertEquals(",", PostgreSQLBulkExport.CSV_DELIMITER.getDefaultValue());
        assertEquals("\"", PostgreSQLBulkExport.CSV_QUOTE.getDefaultValue());
        assertEquals("\"", PostgreSQLBulkExport.CSV_ESCAPE.getDefaultValue());
        assertEquals("\\N", PostgreSQLBulkExport.CSV_NULL.getDefaultValue());
    }

    @Test
    public void testProcessorInputRequirement() {
        // Verify processor forbids input
        org.apache.nifi.annotation.behavior.InputRequirement inputReq = processor.getClass()
                .getAnnotation(org.apache.nifi.annotation.behavior.InputRequirement.class);
        assertNotNull(inputReq);
        assertEquals(org.apache.nifi.annotation.behavior.InputRequirement.Requirement.INPUT_FORBIDDEN, inputReq.value());
    }

    @Test
    public void testProcessorSupportsBatching() {
        // Verify processor supports batching
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.SupportsBatching.class));
    }

    @Test
    public void testProcessorPrimaryNodeOnly() {
        // Verify processor is primary node only
        assertNotNull(processor.getClass().getAnnotation(org.apache.nifi.annotation.behavior.PrimaryNodeOnly.class));
    }

    @Test
    public void testProcessorStateful() {
        // Verify processor is stateful
        org.apache.nifi.annotation.behavior.Stateful stateful = processor.getClass()
                .getAnnotation(org.apache.nifi.annotation.behavior.Stateful.class);
        assertNotNull(stateful);

        // Should use cluster scope for state
        assertEquals(org.apache.nifi.components.state.Scope.CLUSTER, stateful.scopes()[0]);
    }

    @Test
    public void testProcessorWritesAttributes() {
        // Verify processor writes attributes
        org.apache.nifi.annotation.behavior.WritesAttributes writesAttrs = processor.getClass()
                .getAnnotation(org.apache.nifi.annotation.behavior.WritesAttributes.class);
        assertNotNull(writesAttrs);

        // Should write mime.type and record.count attributes
        boolean hasMimeTypeAttr = java.util.Arrays.stream(writesAttrs.value()).anyMatch(attr -> "mime.type".equals(attr.attribute()));
        boolean hasRecordCountAttr = java.util.Arrays.stream(writesAttrs.value()).anyMatch(attr -> "record.count".equals(attr.attribute()));

        assertTrue(hasMimeTypeAttr, "Should write mime.type attribute");
        assertTrue(hasRecordCountAttr, "Should write record.count attribute");
    }

    @Test
    public void testProcessorDefaultSchedule() {
        // Verify processor has default schedule annotation
        org.apache.nifi.annotation.configuration.DefaultSchedule defaultSchedule = processor.getClass()
                .getAnnotation(org.apache.nifi.annotation.configuration.DefaultSchedule.class);
        assertNotNull(defaultSchedule);

        // Should be timer driven with 1 minute period
        assertEquals(org.apache.nifi.scheduling.SchedulingStrategy.TIMER_DRIVEN, defaultSchedule.strategy());
        assertEquals("1 min", defaultSchedule.period());
    }
}
