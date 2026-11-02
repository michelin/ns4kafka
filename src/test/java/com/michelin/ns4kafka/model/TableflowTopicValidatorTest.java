/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.validation.FieldValidationException;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TableflowTopicValidator;
import io.micronaut.json.JsonMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TableflowTopicValidatorTest {
    private static TableflowTopic topic(List<String> formats, String storage) {
        return TableflowTopic.builder()
                .metadata(Resource.Metadata.builder().name("orders").build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(formats)
                        .storage(
                                storage == null
                                        ? null
                                        : TableflowTopic.Storage.builder()
                                                .kind(storage)
                                                .build())
                        .build())
                .build();
    }

    private static TableflowTopicValidator validator(String... formats) {
        return TableflowTopicValidator.builder()
                .validationConstraints(Map.of(
                        "table.format", ResourceValidator.ValidList.in(formats),
                        "storage.kind", ResourceValidator.ValidString.in("Managed")))
                .build();
    }

    @Test
    void shouldAcceptValuesAllowedByTheNamespace() {
        assertFalse(validator("ICEBERG")
                .validate(topic(List.of("ICEBERG"), "Managed"))
                .hasErrors());
    }

    @ParameterizedTest
    @ValueSource(strings = {"DELTA", "OTHER", "iceberg"})
    void shouldRejectFormatsOutsideTheNamespaceAllowlist(String format) {
        var result = validator("ICEBERG").validate(topic(List.of(format), "Managed"));

        assertEquals(1, result.errors().size());
        assertTrue(result.errors().getFirst().contains("table.format"));
        assertTrue(result.errors().getFirst().contains(format));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ByobAws", "AzureDataLakeStorageGen2", "GoogleCloudStorage", "Custom", "managed", ""})
    void shouldRejectStorageOutsideTheNamespaceAllowlist(String storage) {
        var result = validator("ICEBERG").validate(topic(List.of("ICEBERG"), storage));

        assertEquals(1, result.errors().size());
        assertTrue(result.errors().getFirst().contains("storage.kind"));
    }

    @Test
    void shouldValidateEveryRequestedFormat() {
        assertTrue(validator("ICEBERG")
                .validate(topic(List.of("ICEBERG", "DELTA"), "Managed"))
                .hasErrors());
        assertFalse(validator("ICEBERG", "DELTA")
                .validate(topic(List.of("ICEBERG", "DELTA"), "Managed"))
                .hasErrors());
    }

    @Test
    void shouldUseConfiguredValuesRatherThanHardCodedDefaults() {
        TableflowTopicValidator delta = TableflowTopicValidator.builder()
                .validationConstraints(Map.of(
                        "table.format", ResourceValidator.ValidList.in("DELTA"),
                        "storage.kind", ResourceValidator.ValidString.in("ByobAws")))
                .build();

        assertFalse(delta.validate(topic(List.of("DELTA"), "ByobAws")).hasErrors());
        assertEquals(
                2, delta.validate(topic(List.of("ICEBERG"), "Managed")).errors().size());
    }

    @Test
    void shouldNotAddConstraintsWhenNoneAreConfigured() {
        var result = TableflowTopicValidator.builder().build().validate(topic(List.of("DELTA"), "ByobAws"));

        assertEquals(List.of(), result.errors());
        assertEquals(List.of(), result.warnings());
    }

    @Test
    void shouldReportMissingRequiredConstraintValues() {
        var result = validator("ICEBERG").validate(topic(null, null));

        assertEquals(2, result.errors().size());
    }

    @Test
    void shouldKeepSoftValidationFailuresAsWarnings() {
        ResourceValidator.Validator softRule = (name, value) -> {
            throw new FieldValidationException("Prefer Iceberg", true);
        };
        var result = TableflowTopicValidator.builder()
                .validationConstraints(Map.of("table.format", softRule))
                .build()
                .validate(topic(List.of("DELTA"), "Managed"));

        assertEquals(List.of(), result.errors());
        assertEquals(List.of("Prefer Iceberg"), result.warnings());
    }

    @Test
    void shouldReadAndRoundTripNamespaceValidationRules() throws Exception {
        var mapper = JsonMapper.createDefault();
        Namespace namespace = mapper.readValue("""
                {
                  "metadata": {"name": "sales", "cluster": "cloud"},
                  "spec": {
                    "kafkaUser": "user",
                    "tableflowTopicValidator": {
                      "validationConstraints": {
                        "table.format": {"validation-type": "ValidList", "validStrings": ["ICEBERG"]},
                        "storage.kind": {"validation-type": "ValidString", "validStrings": ["Managed"]}
                      }
                    }
                  }
                }
                """, Namespace.class);
        Namespace restored = mapper.readValue(mapper.writeValueAsBytes(namespace), Namespace.class);

        assertEquals(namespace, restored);
        TableflowTopicValidator restoredValidator = restored.getSpec().getTableflowTopicValidator();
        assertFalse(
                restoredValidator.validate(topic(List.of("ICEBERG"), "Managed")).hasErrors());
        assertTrue(
                restoredValidator.validate(topic(List.of("DELTA"), "Managed")).hasErrors());
        assertTrue(
                restoredValidator.validate(topic(List.of("ICEBERG"), "ByobAws")).hasErrors());
    }
}
