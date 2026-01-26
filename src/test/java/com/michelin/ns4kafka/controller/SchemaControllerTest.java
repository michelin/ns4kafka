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
package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SubjectConfigState;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.SchemaService;
import com.michelin.ns4kafka.service.client.schema.entities.SubjectConfigResponse;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class SchemaControllerTest {
    @Mock
    NamespaceService namespaceService;

    @Mock
    SchemaService schemaService;

    @InjectMocks
    SchemaController schemaController;

    @Mock
    SecurityService securityService;

    @Mock
    ApplicationEventPublisher<AuditLog> applicationEventPublisher;

    @Test
    void shouldCreateSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.validateSchema(namespace, schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schema.getMetadata().getName()))
                .thenReturn(Flux.empty());
        when(schemaService.existInOldVersions(namespace, schema, Collections.emptyList()))
                .thenReturn(Mono.just(false));
        when(schemaService.register(namespace, schema)).thenReturn(Mono.just(1));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
                .consumeNextWith(response -> {
                    assertEquals("created", response.header("X-Ns4kafka-Result"));
                    assertTrue(response.getBody().isPresent());
                    assertEquals(
                            "prefix.subject-value",
                            response.getBody().get().getMetadata().getName());
                })
                .verifyComplete();
    }

    @Test
    void shouldChangeSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        Schema schemaV2 = buildSchemaV2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.validateSchema(namespace, schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.validateSchemaCompatibility("local", schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(
                        namespace, schemaV2.getMetadata().getName()))
                .thenReturn(Flux.just(schema));
        when(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
                .thenReturn(Mono.just(false));
        when(schemaService.register(namespace, schemaV2)).thenReturn(Mono.just(2));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        StepVerifier.create(schemaController.apply("myNamespace", schemaV2, false))
                .consumeNextWith(response -> {
                    assertEquals("changed", response.header("X-Ns4kafka-Result"));
                    assertTrue(response.getBody().isPresent());
                    assertEquals(
                            "prefix.subject-value",
                            response.getBody().get().getMetadata().getName());
                })
                .verifyComplete();
    }

    @Test
    void shouldNotChangeSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.validateSchema(namespace, schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schema.getMetadata().getName()))
                .thenReturn(Flux.just(schema));
        when(schemaService.existInOldVersions(namespace, schema, List.of(schema)))
                .thenReturn(Mono.just(true));

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
                .consumeNextWith(response -> {
                    assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
                    assertTrue(response.getBody().isPresent());
                    assertEquals(
                            "prefix.subject-value",
                            response.getBody().get().getMetadata().getName());
                })
                .verifyComplete();
    }

    @Test
    void shouldNotCreateSchemaWhenNotOwner() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(false);

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            1,
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .size());
                    assertEquals(
                            "Invalid value \"prefix.subject-value\" for field \"name\": "
                                    + "namespace is not owner of the resource.",
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .getFirst());
                })
                .verify();
    }

    @Test
    void shouldNotApplySchemaWhenValidationErrors() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.validateSchema(namespace, schema)).thenReturn(Mono.just(List.of("Errors")));

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            1,
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .size());
                    assertEquals(
                            "Errors",
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .getFirst());
                })
                .verify();
    }

    @Test
    void shouldNotCreateSchemaInDryRunMode() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.validateSchema(namespace, schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schema.getMetadata().getName()))
                .thenReturn(Flux.empty());
        when(schemaService.existInOldVersions(namespace, schema, Collections.emptyList()))
                .thenReturn(Mono.just(false));

        StepVerifier.create(schemaController.apply("myNamespace", schema, true))
                .consumeNextWith(response -> {
                    assertEquals("created", response.header("X-Ns4kafka-Result"));
                    assertTrue(response.getBody().isPresent());
                    assertEquals(
                            "prefix.subject-value",
                            response.getBody().get().getMetadata().getName());
                })
                .verifyComplete();

        verify(schemaService, never()).register(namespace, schema);
    }

    @Test
    void shouldChangeSchemaInDryRunMode() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        Schema schemaV2 = buildSchemaV2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.validateSchema(namespace, schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.validateSchemaCompatibility("local", schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(
                        namespace, schemaV2.getMetadata().getName()))
                .thenReturn(Flux.just(schema));
        when(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
                .thenReturn(Mono.just(false));

        StepVerifier.create(schemaController.apply("myNamespace", schemaV2, true))
                .consumeNextWith(response -> {
                    assertEquals("changed", response.header("X-Ns4kafka-Result"));
                    assertTrue(response.getBody().isPresent());
                    assertEquals(
                            "prefix.subject-value",
                            response.getBody().get().getMetadata().getName());
                })
                .verifyComplete();

        verify(schemaService, never()).register(namespace, schemaV2);
    }

    @Test
    void shouldNotCreateSchemaInDryRunModeWhenNotCompatible() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        Schema schemaV2 = buildSchemaV2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schemaV2.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.validateSchema(namespace, schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(
                        namespace, schemaV2.getMetadata().getName()))
                .thenReturn(Flux.just(schema));
        when(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
                .thenReturn(Mono.just(false));
        when(schemaService.validateSchemaCompatibility("local", schemaV2))
                .thenReturn(Mono.just(List.of("Not compatible")));

        StepVerifier.create(schemaController.apply("myNamespace", schemaV2, true))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            1,
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .size());
                    assertEquals(
                            "Not compatible",
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .getFirst());
                })
                .verify();

        verify(schemaService, never()).register(namespace, schema);
    }

    @Test
    void shouldListMultipleSchemas() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchemaNameOnly();
        Schema schema2 = buildSchemaNameOnly2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "*")).thenReturn(Flux.fromIterable(List.of(schema, schema2)));

        StepVerifier.create(schemaController.list("myNamespace", "*"))
                .consumeNextWith(schemaResponse -> assertEquals(
                        "prefix.subject-value", schemaResponse.getMetadata().getName()))
                .consumeNextWith(schemaResponse -> assertEquals(
                        "prefix.subject2-value", schemaResponse.getMetadata().getName()))
                .verifyComplete();
        verify(schemaService, never()).getSubjectLatestVersion(any(), any());
    }

    @Test
    void shouldListOneSchemaWithNameParameter() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchemaNameOnly();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject-value"))
                .thenReturn(Flux.fromIterable(List.of(schema)));
        when(schemaService.getSubjectLatestVersion(namespace, "prefix.subject-value"))
                .thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.list("myNamespace", "prefix.subject-value"))
                .consumeNextWith(schemaResponse -> assertEquals(
                        "prefix.subject-value", schemaResponse.getMetadata().getName()))
                .verifyComplete();
    }

    @Test
    void shouldListSchemaWhenNoSchema() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject-value"))
                .thenReturn(Flux.fromIterable(List.of()));

        StepVerifier.create(schemaController.list("myNamespace", "prefix.subject-value"))
                .verifyComplete();
        verify(schemaService, never()).getSubjectLatestVersion(any(), any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldGetSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.getSubjectLatestVersion(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.get("myNamespace", "prefix.subject-value"))
                .consumeNextWith(response -> assertEquals(
                        "prefix.subject-value", response.getMetadata().getName()))
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotGetSchemaWhenNotOwner() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(false);

        StepVerifier.create(schemaController.get("myNamespace", "prefix.subject-value"))
                .verifyComplete();

        verify(schemaService, never())
                .getSubjectLatestVersion(namespace, schema.getMetadata().getName());
    }

    @Test
    void shouldUpdateAlias() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SubjectConfigResponse oldConfig = SubjectConfigResponse.builder()
                .compatibilityLevel(Schema.Compatibility.FORWARD_TRANSITIVE)
                .alias("myOldAlias-value")
                .build();
        SubjectConfigResponse newConfig = SubjectConfigResponse.builder()
                .compatibilityLevel(Schema.Compatibility.FORWARD_TRANSITIVE)
                .alias("myNewAlias-value")
                .build();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(
                        namespace, schema.getMetadata().getName()))
                .thenReturn(true);
        when(schemaService.getSubjectConfig(namespace, "prefix.subject-value")).thenReturn(Mono.just(oldConfig));
        when(schemaService.updateSubjectConfig(any(), any())).thenReturn(Mono.just(newConfig));

        StepVerifier.create(schemaController.config("myNamespace", "prefix.subject-value", null, "myNewAlias-value"))
                .consumeNextWith(response -> {
                    assertEquals(HttpStatus.OK, response.getStatus());
                    assertTrue(response.getBody().isPresent());
                    assertEquals(
                            "prefix.subject-value",
                            response.getBody().get().getMetadata().getName());
                    assertEquals(
                            Schema.Compatibility.FORWARD_TRANSITIVE,
                            response.getBody().get().getSpec().getCompatibility());
                    assertEquals(
                            "myNewAlias-value",
                            response.getBody().get().getSpec().getAlias());
                })
                .verifyComplete();
    }

    @ParameterizedTest
    @CsvSource({
        "FORWARD,BACKWARD,myAlias,myAlias",
        "FORWARD,FORWARD,myAlias,myNewAlias",
        "FORWARD,BACKWARD,myAlias,myNewAlias"
    })
    void shouldUpdateConfig(
            Schema.Compatibility oldCompatibility,
            Schema.Compatibility newCompatibility,
            String oldAlias,
            String newAlias) {
        Namespace namespace = buildNamespace();
        SubjectConfigResponse oldConfig = SubjectConfigResponse.builder()
                .compatibilityLevel(oldCompatibility)
                .alias(oldAlias)
                .build();
        SubjectConfigResponse newConfig = SubjectConfigResponse.builder()
                .compatibilityLevel(newCompatibility)
                .alias(newAlias)
                .build();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(true);
        when(schemaService.getSubjectConfig(namespace, "prefix.subject-value")).thenReturn(Mono.just(oldConfig));
        when(schemaService.updateSubjectConfig(any(), any())).thenReturn(Mono.just(newConfig));

        StepVerifier.create(schemaController.config("myNamespace", "prefix.subject-value", newCompatibility, newAlias))
                .consumeNextWith(response -> {
                    assertEquals(HttpStatus.OK, response.getStatus());
                    assertTrue(response.getBody().isPresent());
                    assertEquals(
                            "prefix.subject-value",
                            response.getBody().get().getMetadata().getName());
                    assertEquals(
                            newCompatibility, response.getBody().get().getSpec().getCompatibility());
                    assertEquals(newAlias, response.getBody().get().getSpec().getAlias());
                })
                .verifyComplete();
    }

    @ParameterizedTest
    @CsvSource({"FORWARD,FORWARD,,", ",,myAlias,myAlias", "FORWARD,FORWARD,myAlias,myAlias"})
    void shouldNotUpdateConfigWhenSameConfig(
            Schema.Compatibility oldCompatibility,
            Schema.Compatibility newCompatibility,
            String oldAlias,
            String newAlias) {
        Namespace namespace = buildNamespace();
        SubjectConfigResponse oldConfig = SubjectConfigResponse.builder()
                .compatibilityLevel(oldCompatibility)
                .alias(oldAlias)
                .build();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(true);
        when(schemaService.getSubjectConfig(namespace, "prefix.subject-value")).thenReturn(Mono.just(oldConfig));

        StepVerifier.create(schemaController.config("myNamespace", "prefix.subject-value", newCompatibility, newAlias))
                .consumeNextWith(response -> {
                    assertEquals(HttpStatus.OK, response.getStatus());
                    assertTrue(response.getBody().isPresent());
                    assertEquals(
                            "prefix.subject-value",
                            response.getBody().get().getMetadata().getName());
                    assertEquals(
                            newCompatibility, response.getBody().get().getSpec().getCompatibility());
                    assertEquals(newAlias, response.getBody().get().getSpec().getAlias());
                })
                .verifyComplete();

        verify(schemaService, never()).updateSubjectConfig(any(), any());
    }

    @Test
    void shouldNotUpdateConfigWhenNamespaceNotOwner() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(false);

        StepVerifier.create(schemaController.config(
                        "myNamespace", "prefix.subject-value", Schema.Compatibility.BACKWARD, null))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            1,
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .size());
                    assertEquals(
                            "Invalid value \"prefix.subject-value\" for field \"name\": "
                                    + "namespace is not owner of the resource.",
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .getFirst());
                })
                .verify();

        verify(schemaService, never()).updateSubjectConfig(any(), any());
    }

    @Test
    void shouldNotUpdateConfigWhenNamespaceNotOwnerOfAlias() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(true);
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "other.subject-value"))
                .thenReturn(false);

        StepVerifier.create(schemaController.config("myNamespace", "prefix.subject-value", null, "other.subject-value"))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            1,
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .size());
                    assertEquals(
                            "Invalid value \"other.subject-value\" for field \"alias\": "
                                    + "namespace is not owner of the resource.",
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .getFirst());
                })
                .verify();

        verify(schemaService, never()).updateSubjectConfig(any(), any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteAllSchemaVersionsWhenNotOwner() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(false);

        StepVerifier.create(schemaController.delete("myNamespace", "prefix.subject-value", Optional.empty(), false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            1,
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .size());
                    assertEquals(
                            "Invalid value \"prefix.subject-value\" for field \"name\": "
                                    + "namespace is not owner of the resource.",
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .getFirst());
                })
                .verify();

        verify(schemaService, never()).getSubjectLatestVersion(any(), any());
        verify(schemaService, never()).deleteAllVersions(any(), any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteOneSchemaVersionWhenNotOwner() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(false);

        StepVerifier.create(schemaController.delete("myNamespace", "prefix.subject-value", Optional.of("1"), false))
                .consumeErrorWith(error -> {
                    assertEquals(ResourceValidationException.class, error.getClass());
                    assertEquals(
                            1,
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .size());
                    assertEquals(
                            "Invalid value \"prefix.subject-value\" for field \"name\": "
                                    + "namespace is not owner of the resource.",
                            ((ResourceValidationException) error)
                                    .getValidationErrors()
                                    .getFirst());
                })
                .verify();

        verify(schemaService, never()).getSubjectByVersion(any(), any(), any());
        verify(schemaService, never()).deleteVersion(any(), any(), any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteAllSchemaVersions() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(true);
        when(schemaService.getSubjectLatestVersion(namespace, "prefix.subject-value"))
                .thenReturn(Mono.just(schema));
        when(schemaService.deleteAllVersions(namespace, "prefix.subject-value")).thenReturn(Mono.just(new Integer[1]));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        StepVerifier.create(schemaController.delete("myNamespace", "prefix.subject-value", Optional.empty(), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NO_CONTENT, response.getStatus()))
                .verifyComplete();

        verify(schemaService).deleteAllVersions(namespace, "prefix.subject-value");
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldDeleteSchemaVersion() {
        Namespace namespace = buildNamespace();
        Schema schema1 = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(true);
        when(schemaService.getSubjectByVersion(namespace, "prefix.subject-value", "1"))
                .thenReturn(Mono.just(schema1));
        when(schemaService.deleteVersion(namespace, "prefix.subject-value", "1"))
                .thenReturn(Mono.just(1));

        StepVerifier.create(schemaController.delete("myNamespace", "prefix.subject-value", Optional.of("1"), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NO_CONTENT, response.getStatus()))
                .verifyComplete();

        verify(applicationEventPublisher).publishEvent(any());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteAllSchemaVersionsWhenEmpty() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(true);
        when(schemaService.getSubjectLatestVersion(namespace, "prefix.subject-value"))
                .thenReturn(Mono.empty());

        StepVerifier.create(schemaController.delete("myNamespace", "prefix.subject-value", Optional.empty(), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteAllVersions(namespace, "prefix.subject-value");
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteSchemaVersionWhenEmpty() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(true);
        when(schemaService.getSubjectByVersion(namespace, "prefix.subject-value", "1"))
                .thenReturn(Mono.empty());

        StepVerifier.create(schemaController.delete("myNamespace", "prefix.subject-value", Optional.of("1"), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteVersion(namespace, "prefix.subject-value", "1");
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteAllSchemaVersionsInDryRunMode() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(true);
        when(schemaService.getSubjectLatestVersion(namespace, "prefix.subject-value"))
                .thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.delete("myNamespace", "prefix.subject-value", Optional.empty(), true))
                .consumeNextWith(response -> assertEquals(HttpStatus.NO_CONTENT, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteAllVersions(namespace, "prefix.subject-value");
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldNotDeleteSchemaVersionInDryRunMode() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value"))
                .thenReturn(true);
        when(schemaService.getSubjectByVersion(namespace, "prefix.subject-value", "1"))
                .thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.delete("myNamespace", "prefix.subject-value", Optional.of("1"), true))
                .consumeNextWith(response -> assertEquals(HttpStatus.NO_CONTENT, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteVersion(namespace, "prefix.subject-value", "1");
    }

    @Test
    void shouldBulkDeleteAllSchemaVersions() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchemaNameOnly();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject-value"))
                .thenReturn(Flux.fromIterable(List.of(schema)));
        when(schemaService.getSubjectLatestVersion(namespace, "prefix.subject-value"))
                .thenReturn(Mono.just(schema));
        when(schemaService.deleteAllVersions(namespace, "prefix.subject-value")).thenReturn(Mono.just(new Integer[1]));

        StepVerifier.create(schemaController.bulkDelete("myNamespace", "prefix.subject-value", Optional.empty(), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.OK, response.getStatus()))
                .verifyComplete();

        verify(applicationEventPublisher).publishEvent(any());
    }

    @Test
    void shouldBulkDeleteSchemaVersion() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchemaNameOnly();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject-value"))
                .thenReturn(Flux.fromIterable(List.of(schema)));
        when(schemaService.getSubjectByVersion(namespace, "prefix.subject-value", "1"))
                .thenReturn(Mono.just(schema));
        when(schemaService.deleteVersion(namespace, "prefix.subject-value", "1"))
                .thenReturn(Mono.just(1));

        StepVerifier.create(schemaController.bulkDelete("myNamespace", "prefix.subject-value", Optional.of("1"), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.OK, response.getStatus()))
                .verifyComplete();

        verify(applicationEventPublisher).publishEvent(any());
    }

    @Test
    void shouldNotBulkDeleteAllSchemaVersionsWhenEmpty() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject-value"))
                .thenReturn(Flux.fromIterable(List.of()));

        StepVerifier.create(schemaController.bulkDelete("myNamespace", "prefix.subject-value", Optional.empty(), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteAllVersions(namespace, "prefix.subject-value");
    }

    @Test
    void shouldNotBulkDeleteSchemaVersionWhenEmpty() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject-value"))
                .thenReturn(Flux.fromIterable(List.of()));

        StepVerifier.create(schemaController.bulkDelete("myNamespace", "prefix.subject-value", Optional.of("1"), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteVersion(namespace, "prefix.subject-value", "1");
    }

    @Test
    void shouldNotBulkDeleteAllSchemaVersionsWhenVersionNotFound() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchemaNameOnly();
        Schema schema2 = buildSchemaNameOnly2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject*"))
                .thenReturn(Flux.fromIterable(List.of(schema, schema2)));
        when(schemaService.getSubjectLatestVersion(namespace, "prefix.subject-value"))
                .thenReturn(Mono.just(schema));
        when(schemaService.getSubjectLatestVersion(namespace, "prefix.subject2-value"))
                .thenReturn(Mono.empty());

        StepVerifier.create(schemaController.bulkDelete("myNamespace", "prefix.subject*", Optional.empty(), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteAllVersions(namespace, "prefix.subject-value");
        verify(schemaService, never()).deleteAllVersions(namespace, "prefix.subject2-value");
    }

    @Test
    void shouldNotBulkDeleteSchemaVersionWhenVersionNotFound() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchemaNameOnly();
        Schema schema2 = buildSchemaNameOnly2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject*"))
                .thenReturn(Flux.fromIterable(List.of(schema, schema2)));
        when(schemaService.getSubjectByVersion(namespace, "prefix.subject-value", "1"))
                .thenReturn(Mono.just(schema));
        when(schemaService.getSubjectByVersion(namespace, "prefix.subject2-value", "1"))
                .thenReturn(Mono.empty());

        StepVerifier.create(schemaController.bulkDelete("myNamespace", "prefix.subject*", Optional.of("1"), false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteVersion(namespace, "prefix.subject-value", "1");
        verify(schemaService, never()).deleteVersion(namespace, "prefix.subject2-value", "1");
    }

    @Test
    void shouldNotBulkDeleteAllSchemaVersionsInDryRunMode() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchemaNameOnly();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject-value"))
                .thenReturn(Flux.fromIterable(List.of(schema)));
        when(schemaService.getSubjectLatestVersion(namespace, "prefix.subject-value"))
                .thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.bulkDelete("myNamespace", "prefix.subject-value", Optional.empty(), true))
                .consumeNextWith(response -> assertEquals(HttpStatus.OK, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteAllVersions(namespace, "prefix.subject-value");
    }

    @Test
    void shouldNotBulkDeleteSchemaVersionInDryRunMode() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchemaNameOnly();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findByWildcardName(namespace, "prefix.subject-value"))
                .thenReturn(Flux.fromIterable(List.of(schema)));
        when(schemaService.getSubjectByVersion(namespace, "prefix.subject-value", "1"))
                .thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.bulkDelete("myNamespace", "prefix.subject-value", Optional.of("1"), true))
                .consumeNextWith(response -> assertEquals(HttpStatus.OK, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).deleteVersion(namespace, "prefix.subject-value", "1");
    }

    private Namespace buildNamespace() {
        return Namespace.builder()
                .metadata(
                        Metadata.builder().name("myNamespace").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder().build())
                .build();
    }

    private Schema buildSchema() {
        return Schema.builder()
                .metadata(Metadata.builder().name("prefix.subject-value").build())
                .spec(Schema.SchemaSpec.builder()
                        .id(1)
                        .version(1)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                                + "\"name\":\"PersonAvro\""
                                + ",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                                + "\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"Last name of the person\"},"
                                + "{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\","
                                + "\"logicalType\":\"timestamp-millis\"}],"
                                + "\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();
    }

    private Schema buildSchemaV2() {
        return Schema.builder()
                .metadata(Metadata.builder().name("prefix.subject-value").build())
                .spec(Schema.SchemaSpec.builder()
                        .id(1)
                        .version(2)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                                + "\"name\":\"PersonAvro\""
                                + ",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                                + "\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\","
                                + "\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},"
                                + "{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\","
                                + "\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                                + "\"doc\":\"Date of birth of the person\"},{\"name\":\"birthPlace\",\"type\":[\"null\","
                                + "\"string\"],\"default\":null,\"doc\":\"Place of birth\"}]}")
                        .build())
                .build();
    }

    private Schema buildSchemaNameOnly() {
        return Schema.builder()
                .metadata(Metadata.builder().name("prefix.subject-value").build())
                .build();
    }

    private Schema buildSchemaNameOnly2() {
        return Schema.builder()
                .metadata(Metadata.builder().name("prefix.subject2-value").build())
                .build();
    }

    private SubjectConfigState buildSubjectState() {
        return SubjectConfigState.builder()
                .metadata(Metadata.builder()
                        .cluster("local")
                        .namespace("myNamespace")
                        .name("prefix.subject-value")
                        .build())
                .spec(SubjectConfigState.SubjectConfigStateSpec.builder()
                        .compatibility(Schema.Compatibility.FORWARD)
                        .build())
                .build();
    }
}
