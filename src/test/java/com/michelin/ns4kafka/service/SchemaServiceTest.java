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
package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaResponse;
import com.michelin.ns4kafka.service.client.schema.entities.SubjectConfigResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class SchemaServiceTest {
    @InjectMocks
    SchemaService schemaService;

    @Mock
    AclService aclService;

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Test
    void shouldListSchemasWithoutParameter() {
        Namespace namespace = buildNamespace();
        List<String> subjectsResponse =
                Arrays.asList("prefix.schema-one", "prefix2.schema-two", "prefix2.schema-three");

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("myNamespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("prefix.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("myNamespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("prefix2.schema-two")
                                .build())
                        .build());

        when(aclService.findResourceOwnerGrantedToNamespace(namespace, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(acls);

        when(schemaRegistryClient.getSubjects(namespace.getMetadata().getCluster()))
                .thenReturn(Flux.fromIterable(subjectsResponse));
        when(aclService.isResourceCoveredByAcls(acls, "prefix.schema-one")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "prefix2.schema-two")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "prefix2.schema-three")).thenReturn(false);

        StepVerifier.create(schemaService.findAllForNamespace(namespace))
                .consumeNextWith(schema ->
                        assertEquals("prefix.schema-one", schema.getMetadata().getName()))
                .consumeNextWith(schema ->
                        assertEquals("prefix2.schema-two", schema.getMetadata().getName()))
                .verifyComplete();
    }

    @Test
    void shouldListSchemasWhenEmpty() {
        Namespace namespace = buildNamespace();

        when(schemaRegistryClient.getSubjects(namespace.getMetadata().getCluster()))
                .thenReturn(Flux.empty());

        StepVerifier.create(schemaService.findAllForNamespace(namespace)).verifyComplete();
    }

    @Test
    void shouldListSchemaWithNameParameter() {
        Namespace namespace = buildNamespace();
        List<String> subjectsResponse = List.of("prefix.schema-one", "prefix2.schema-two", "prefix2.schema-three");

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("myNamespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("prefix.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("myNamespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("prefix2.schema-two")
                                .build())
                        .build());

        when(aclService.findResourceOwnerGrantedToNamespace(namespace, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(acls);
        when(schemaRegistryClient.getSubjects(namespace.getMetadata().getCluster()))
                .thenReturn(Flux.fromIterable(subjectsResponse));
        when(aclService.isResourceCoveredByAcls(acls, "prefix.schema-one")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "prefix2.schema-two")).thenReturn(true);
        when(aclService.isResourceCoveredByAcls(acls, "prefix2.schema-three")).thenReturn(false);

        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix.schema-one"))
                .consumeNextWith(schema ->
                        assertEquals("prefix.schema-one", schema.getMetadata().getName()))
                .verifyComplete();
        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix2.schema-three"))
                .verifyComplete();
        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix3.schema-four"))
                .verifyComplete();
        StepVerifier.create(schemaService.findByWildcardName(namespace, ""))
                .consumeNextWith(schema ->
                        assertEquals("prefix.schema-one", schema.getMetadata().getName()))
                .consumeNextWith(schema ->
                        assertEquals("prefix2.schema-two", schema.getMetadata().getName()))
                .verifyComplete();
    }

    @Test
    void shouldListSchemaWithWildcardNameParameter() {
        Namespace namespace = buildNamespace();
        List<String> subjectsResponse = List.of(
                "prefix1.schema1-value",
                "prefix1.schema1-key",
                "prefix1.schema-one",
                "prefix2.schema2-value",
                "prefix4.schema1-value",
                "prefix4.schema2-key");

        List<AccessControlEntry> acls = List.of(
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("myNamespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("prefix1.")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("myNamespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("prefix2.schema2")
                                .build())
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .permission(AccessControlEntry.Permission.OWNER)
                                .grantedTo("myNamespace")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .resource("prefix4.")
                                .build())
                        .build());

        when(aclService.findResourceOwnerGrantedToNamespace(namespace, AccessControlEntry.ResourceType.TOPIC))
                .thenReturn(acls);
        when(schemaRegistryClient.getSubjects(namespace.getMetadata().getCluster()))
                .thenReturn(Flux.fromIterable(subjectsResponse));
        when(aclService.isResourceCoveredByAcls(eq(acls), anyString())).thenReturn(true);

        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix1.*"))
                .consumeNextWith(schema -> assertEquals(
                        "prefix1.schema1-value", schema.getMetadata().getName()))
                .consumeNextWith(schema ->
                        assertEquals("prefix1.schema1-key", schema.getMetadata().getName()))
                .consumeNextWith(schema ->
                        assertEquals("prefix1.schema-one", schema.getMetadata().getName()))
                .verifyComplete();

        StepVerifier.create(schemaService.findByWildcardName(namespace, "*-value"))
                .consumeNextWith(schema -> assertEquals(
                        "prefix1.schema1-value", schema.getMetadata().getName()))
                .consumeNextWith(schema -> assertEquals(
                        "prefix2.schema2-value", schema.getMetadata().getName()))
                .consumeNextWith(schema -> assertEquals(
                        "prefix4.schema1-value", schema.getMetadata().getName()))
                .verifyComplete();

        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix?.schema1-key"))
                .consumeNextWith(schema ->
                        assertEquals("prefix1.schema1-key", schema.getMetadata().getName()))
                .verifyComplete();

        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix?.schema-*"))
                .consumeNextWith(schema ->
                        assertEquals("prefix1.schema-one", schema.getMetadata().getName()))
                .verifyComplete();

        StepVerifier.create(schemaService.findByWildcardName(namespace, "*"))
                .consumeNextWith(schema -> assertEquals(
                        "prefix1.schema1-value", schema.getMetadata().getName()))
                .consumeNextWith(schema ->
                        assertEquals("prefix1.schema1-key", schema.getMetadata().getName()))
                .consumeNextWith(schema ->
                        assertEquals("prefix1.schema-one", schema.getMetadata().getName()))
                .consumeNextWith(schema -> assertEquals(
                        "prefix2.schema2-value", schema.getMetadata().getName()))
                .consumeNextWith(schema -> assertEquals(
                        "prefix4.schema1-value", schema.getMetadata().getName()))
                .consumeNextWith(schema ->
                        assertEquals("prefix4.schema2-key", schema.getMetadata().getName()))
                .verifyComplete();
    }

    @Test
    void shouldGetSubjectLatestVersion() {
        Namespace namespace = buildNamespace();
        SubjectConfigResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "prefix.schema-one", "latest"))
                .thenReturn(Mono.just(buildSchemaResponse("prefix.schema-one")));
        when(schemaRegistryClient.getSubjectConfig(any(), any())).thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.getSubjectLatestVersion(namespace, "prefix.schema-one"))
                .consumeNextWith(latestSubject -> {
                    assertEquals(
                            "prefix.schema-one", latestSubject.getMetadata().getName());
                    assertEquals("local", latestSubject.getMetadata().getCluster());
                    assertEquals("myNamespace", latestSubject.getMetadata().getNamespace());
                })
                .verifyComplete();
    }

    @Test
    void shouldGetAllSubjectVersions() {
        Namespace namespace = buildNamespace();
        SchemaResponse schemaResponse = buildSchemaResponse("prefix.schema-one");

        when(schemaRegistryClient.getAllSubjectVersions(namespace.getMetadata().getCluster(), "prefix.schema-one"))
                .thenReturn(Flux.just(schemaResponse));

        StepVerifier.create(schemaService.getAllSubjectVersions(namespace, "prefix.schema-one"))
                .consumeNextWith(subjectVersion -> {
                    assertEquals(
                            "prefix.schema-one", subjectVersion.getMetadata().getName());
                    assertEquals("local", subjectVersion.getMetadata().getCluster());
                    assertEquals("myNamespace", subjectVersion.getMetadata().getNamespace());
                    assertEquals(
                            schemaResponse.references(),
                            subjectVersion.getSpec().getReferences());
                })
                .verifyComplete();
    }

    @Test
    void shouldNotGetSubjectLatestVersionWhenEmpty() {
        Namespace namespace = buildNamespace();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "prefix.schema-one", "latest"))
                .thenReturn(Mono.empty());

        StepVerifier.create(schemaService.getSubjectLatestVersion(namespace, "prefix.schema-one"))
                .verifyComplete();
    }

    @Test
    void shouldRegisterSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.register(any(), any(), any()))
                .thenReturn(Mono.just(SchemaResponse.builder().id(1).version(1).build()));

        StepVerifier.create(schemaService.register(namespace, schema))
                .consumeNextWith(id -> assertEquals(1, id))
                .verifyComplete();
    }

    @Test
    void shouldDeleteSchemaAllVersions() {
        Namespace namespace = buildNamespace();

        when(schemaRegistryClient.deleteSubject(namespace.getMetadata().getCluster(), "prefix.schema-one", false))
                .thenReturn(Mono.just(new Integer[] {1}));

        when(schemaRegistryClient.deleteSubject(namespace.getMetadata().getCluster(), "prefix.schema-one", true))
                .thenReturn(Mono.just(new Integer[] {1}));

        StepVerifier.create(schemaService.deleteAllVersions(namespace, "prefix.schema-one"))
                .consumeNextWith(ids -> {
                    assertEquals(1, ids.length);
                    assertEquals(1, ids[0]);
                })
                .verifyComplete();

        verify(schemaRegistryClient).deleteSubject(namespace.getMetadata().getCluster(), "prefix.schema-one", false);

        verify(schemaRegistryClient).deleteSubject(namespace.getMetadata().getCluster(), "prefix.schema-one", true);
    }

    @Test
    void shouldDeleteSchemaSpecificVersion() {
        Namespace namespace = buildNamespace();
        when(schemaRegistryClient.deleteSubjectVersion(
                        namespace.getMetadata().getCluster(), "prefix.schema-one-value", "2", false))
                .thenReturn(Mono.just(2));

        when(schemaRegistryClient.deleteSubjectVersion(
                        namespace.getMetadata().getCluster(), "prefix.schema-one-value", "2", true))
                .thenReturn(Mono.just(2));

        StepVerifier.create(schemaService.deleteVersion(namespace, "prefix.schema-one-value", "2"))
                .consumeNextWith(version -> assertEquals(2, version))
                .verifyComplete();
    }

    @Test
    void shouldValidateSchemaCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SchemaCompatibilityCheckResponse schemaCompatibilityCheckResponse =
                SchemaCompatibilityCheckResponse.builder().isCompatible(true).build();

        when(schemaRegistryClient.validateSchemaCompatibility(any(), any(), any()))
                .thenReturn(Mono.just(schemaCompatibilityCheckResponse));

        StepVerifier.create(schemaService.validateSchemaCompatibility(
                        namespace.getMetadata().getCluster(), schema))
                .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldNotValidateSchemaCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SchemaCompatibilityCheckResponse schemaCompatibilityCheckResponse = SchemaCompatibilityCheckResponse.builder()
                .isCompatible(false)
                .messages(List.of("Incompatible schema"))
                .build();

        when(schemaRegistryClient.validateSchemaCompatibility(any(), any(), any()))
                .thenReturn(Mono.just(schemaCompatibilityCheckResponse));

        StepVerifier.create(schemaService.validateSchemaCompatibility(
                        namespace.getMetadata().getCluster(), schema))
                .consumeNextWith(errors -> {
                    assertEquals(1, errors.size());
                    assertTrue(errors.contains("Invalid \"prefix.schema-one-value\": incompatible schema."));
                })
                .verifyComplete();
    }

    @Test
    void shouldValidateSchemaCompatibilityWhen404NotFound() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.validateSchemaCompatibility(any(), any(), any()))
                .thenReturn(Mono.empty());

        StepVerifier.create(schemaService.validateSchemaCompatibility(
                        namespace.getMetadata().getCluster(), schema))
                .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldUpdateSchemaCompatibilityWhenResettingToDefault() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.deleteSubjectConfig(any(), any()))
                .thenReturn(Mono.just(SubjectConfigResponse.builder()
                        .compatibilityLevel(Schema.Compatibility.FORWARD)
                        .build()));

        StepVerifier.create(schemaService.updateSubjectConfig(
                        namespace, schema, Schema.Compatibility.GLOBAL, Optional.empty()))
                .consumeNextWith(schemaCompatibilityResponse ->
                        assertEquals(Schema.Compatibility.FORWARD, schemaCompatibilityResponse.compatibilityLevel()))
                .verifyComplete();

        verify(schemaRegistryClient).deleteSubjectConfig(any(), any());
    }

    @Test
    void shouldUpdateSubjectConfig() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.createOrUpdateSubjectConfig(any(), any(), any()))
                .thenReturn(Mono.just(SubjectConfigResponse.builder()
                        .compatibilityLevel(Schema.Compatibility.FORWARD)
                        .build()));

        StepVerifier.create(schemaService.updateSubjectConfig(
                        namespace, schema, Schema.Compatibility.FORWARD, Optional.empty()))
                .consumeNextWith(schemaCompatibilityResponse ->
                        assertEquals(Schema.Compatibility.FORWARD, schemaCompatibilityResponse.compatibilityLevel()))
                .verifyComplete();

        verify(schemaRegistryClient).createOrUpdateSubjectConfig(any(), any(), any());
    }

    @Test
    void shouldNamespaceBeOwnerOfSchema() {
        Namespace ns = buildNamespace();
        when(aclService.isNamespaceOwnerOfResource(
                        "myNamespace", AccessControlEntry.ResourceType.TOPIC, "prefix.schema-one"))
                .thenReturn(true);

        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-key"));
        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-value"));
        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one"));
    }

    @Test
    void shouldValidateSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SubjectConfigResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildSchemaResponse("subject-reference")));
        when(schemaRegistryClient.getSubjectConfig(any(), any())).thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.validateSchema(namespace, schema))
                .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldNotValidateSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        schema.getMetadata().setName("wrongSubjectName");

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.empty());

        StepVerifier.create(schemaService.validateSchema(namespace, schema))
                .consumeNextWith(errors -> {
                    assertTrue(errors.contains("Invalid value \"wrongSubjectName\" for field \"name\": "
                            + "value must end with -key or -value."));
                    assertTrue(errors.contains("Invalid value \"header-value\" for field \"references\": "
                            + "subject header-value version 1 not found."));
                })
                .verifyComplete();
    }

    @Test
    void shouldGetEmptySchemaReferences() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        schema.getSpec().setReferences(Collections.emptyList());

        StepVerifier.create(schemaService.getSchemaReferences(schema, namespace))
                .consumeNextWith(refs -> assertTrue(refs.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldGetSchemaReferences() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SchemaResponse schemaResponse = buildReferenceSchemaResponse("header-value");
        SubjectConfigResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(schemaResponse));
        when(schemaRegistryClient.getSubjectConfig(any(), any())).thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.getSchemaReferences(schema, namespace))
                .consumeNextWith(refs -> assertTrue(
                        refs.containsKey(schemaResponse.subject()) && refs.containsValue(schemaResponse.schema())))
                .verifyComplete();
    }

    @Test
    void shouldBeEqualByCanonicalStringAndRefs() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SubjectConfigResponse compatibilityResponse = buildCompatibilityResponse();
        Schema schemaV2 = buildSchemaV2();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildReferenceSchemaResponse("header-value")));
        when(schemaRegistryClient.getSubjectConfig(any(), any())).thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.existInOldVersions(namespace, schema, List.of(schema, schemaV2)))
                .consumeNextWith(Assertions::assertTrue)
                .verifyComplete();
    }

    @Test
    void shouldBeEqualByCanonicalString() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        schema.getSpec().setReferences(Collections.emptyList());
        Schema schemaV2 = buildSchemaV2();

        StepVerifier.create(schemaService.existInOldVersions(namespace, schema, List.of(schema, schemaV2)))
                .consumeNextWith(Assertions::assertTrue)
                .verifyComplete();
    }

    @Test
    void shouldNotBeEqualByCanonicalString() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        schema.getSpec().setReferences(Collections.emptyList());
        Schema schemaV2 = buildSchemaV2();

        StepVerifier.create(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
                .consumeNextWith(Assertions::assertFalse)
                .verifyComplete();
    }

    @Test
    void shouldNotBeEqualByCanonicalStringAndRefs() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        Schema schemaV2 = buildSchemaV2();
        SubjectConfigResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildReferenceSchemaResponse("header-value")));
        when(schemaRegistryClient.getSubjectConfig(any(), any())).thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
                .consumeNextWith(Assertions::assertFalse)
                .verifyComplete();
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
                .metadata(Metadata.builder().name("prefix.schema-one-value").build())
                .spec(Schema.SchemaSpec.builder()
                        .compatibility(Schema.Compatibility.BACKWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                                + "\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"First name of the person\"},"
                                + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                                + "\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\","
                                + "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                                + "\"doc\":\"Date of birth of the person\"}]}")
                        .references(List.of(Schema.SchemaSpec.Reference.builder()
                                .name("HeaderAvro")
                                .subject("header-value")
                                .version(1)
                                .build()))
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

    private SchemaResponse buildSchemaResponse(String subject) {
        return SchemaResponse.builder()
                .id(1)
                .version(1)
                .subject(subject)
                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                        + "\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                        + "\"default\":null,\"doc\":\"First name of the person\"},"
                        + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                        + "\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\","
                        + "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                        + "\"doc\":\"Date of birth of the person\"}]}")
                .schemaType("AVRO")
                .references(List.of(Schema.SchemaSpec.Reference.builder()
                        .name("HeaderAvro")
                        .subject("header-value")
                        .version(1)
                        .build()))
                .build();
    }

    private SchemaResponse buildReferenceSchemaResponse(String subject) {
        return SchemaResponse.builder()
                .id(1)
                .version(1)
                .subject(subject)
                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                        + "\"name\":\"Header\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],"
                        + "\"default\":null,\"doc\":\"Header ID\"}]}")
                .build();
    }

    private SubjectConfigResponse buildCompatibilityResponse() {
        return SubjectConfigResponse.builder()
                .compatibilityLevel(Schema.Compatibility.BACKWARD)
                .build();
    }
}
