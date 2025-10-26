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

import static com.michelin.ns4kafka.model.schema.SubjectNameStrategy.*;
import static com.michelin.ns4kafka.util.config.TopicConfig.KEY_SUBJECT_NAME_STRATEGY;
import static com.michelin.ns4kafka.util.config.TopicConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaResponse;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class SchemaServiceTest {
    @Mock
    List<ManagedClusterProperties> managedClusterProperties;

    @Mock
    AclService aclService;

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @InjectMocks
    SchemaService schemaService;

    @Test
    void shouldListSchemasWithoutParameter() {
        Namespace namespace = buildNamespace();
        List<String> subjectsResponse =
                Arrays.asList("prefix.schema-one-value", "prefix2.schema-two-value", "prefix2.schema-three-value");

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
                .consumeNextWith(schema -> assertEquals(
                        "prefix.schema-one-value", schema.getMetadata().getName()))
                .consumeNextWith(schema -> assertEquals(
                        "prefix2.schema-two-value", schema.getMetadata().getName()))
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
        List<String> subjectsResponse =
                List.of("prefix.schema-one-value", "prefix2.schema-two-value", "prefix2.schema-three-value");

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

        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix.schema-one-value"))
                .consumeNextWith(schema -> assertEquals(
                        "prefix.schema-one-value", schema.getMetadata().getName()))
                .verifyComplete();
        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix2.schema-three"))
                .verifyComplete();
        StepVerifier.create(schemaService.findByWildcardName(namespace, "prefix3.schema-four"))
                .verifyComplete();
        StepVerifier.create(schemaService.findByWildcardName(namespace, ""))
                .consumeNextWith(schema -> assertEquals(
                        "prefix.schema-one-value", schema.getMetadata().getName()))
                .consumeNextWith(schema -> assertEquals(
                        "prefix2.schema-two-value", schema.getMetadata().getName()))
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
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "prefix.schema-one", "latest"))
                .thenReturn(Mono.just(buildSchemaResponse("prefix.schema-one")));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

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
        Schema schema = buildSchema("prefix.schema-one-value");

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
        Schema schema = buildSchema("prefix.schema-one-value");
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
        Schema schema = buildSchema("prefix.schema-one-value");
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
        Schema schema = buildSchema("prefix.schema-one-value");

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
        Schema schema = buildSchema("prefix.schema-one-value");

        when(schemaRegistryClient.deleteCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(SchemaCompatibilityResponse.builder()
                        .compatibilityLevel(Schema.Compatibility.FORWARD)
                        .build()));

        StepVerifier.create(schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.GLOBAL))
                .consumeNextWith(schemaCompatibilityResponse ->
                        assertEquals(Schema.Compatibility.FORWARD, schemaCompatibilityResponse.compatibilityLevel()))
                .verifyComplete();

        verify(schemaRegistryClient).deleteCurrentCompatibilityBySubject(any(), any());
    }

    @Test
    void shouldUpdateSubjectCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("prefix.schema-one-value");

        when(schemaRegistryClient.updateSubjectCompatibility(any(), any(), any()))
                .thenReturn(Mono.just(SchemaCompatibilityResponse.builder()
                        .compatibilityLevel(Schema.Compatibility.FORWARD)
                        .build()));

        StepVerifier.create(schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD))
                .consumeNextWith(schemaCompatibilityResponse ->
                        assertEquals(Schema.Compatibility.FORWARD, schemaCompatibilityResponse.compatibilityLevel()))
                .verifyComplete();

        verify(schemaRegistryClient).updateSubjectCompatibility(any(), any(), any());
    }

    @Test
    void shouldNamespaceBeOwnerOfSchema() {
        Namespace ns = buildNamespace();
        when(aclService.isNamespaceOwnerOfResource(
                        eq("myNamespace"),
                        eq(AccessControlEntry.ResourceType.TOPIC),
                        argThat(arg -> arg.equals("prefix.schema-one") || arg.equals("com.michelin.User"))))
                .thenReturn(true);

        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-key"));
        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-value"));
        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-com.michelin.User"));
        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "com.michelin.User"));
    }

    @Test
    void shouldValidateSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("prefix.schema-one-value");
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildSchemaResponse("subject-reference")));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.validateSchema(namespace, schema))
                .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
                .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("subjectStrategiesForConfluentCloud")
    void shouldVerifySubjectCompatibilityOnConfluentCloud(
            Map<String, ResourceValidator.Validator> validators,
            String subjectName,
            String schemaContent,
            boolean expectedResult) {
        TopicValidator topicValidator =
                TopicValidator.builder().validationConstraints(validators).build();

        Namespace namespace = Namespace.builder()
                .metadata(
                        Metadata.builder().name("myNamespace").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(topicValidator)
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(Metadata.builder().name(subjectName).build())
                .spec(Schema.SchemaSpec.builder()
                        .schema(schemaContent)
                        .references(List.of(Schema.SchemaSpec.Reference.builder()
                                .name("HeaderAvro")
                                .subject("header-value")
                                .version(1)
                                .build()))
                        .build())
                .build();
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(managedClusterProperties.stream())
                .thenReturn(Stream.of(
                        new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD)));
        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildSchemaResponse("subject-reference")));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.validateSchema(namespace, schema))
                .consumeNextWith(errors -> assertEquals(expectedResult, errors.isEmpty()))
                .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("subjectStrategiesForSelfManaged")
    void shouldVerifySubjectCompatibilityOnSelfManaged(
            String subjectName, String schemaContent, boolean expectedResult) {
        Namespace namespace = Namespace.builder()
                .metadata(
                        Metadata.builder().name("myNamespace").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(Metadata.builder().name(subjectName).build())
                .spec(Schema.SchemaSpec.builder()
                        .schema(schemaContent)
                        .references(List.of(Schema.SchemaSpec.Reference.builder()
                                .name("HeaderAvro")
                                .subject("header-value")
                                .version(1)
                                .build()))
                        .build())
                .build();
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(managedClusterProperties.stream())
                .thenReturn(Stream.of(
                        new ManagedClusterProperties("local", ManagedClusterProperties.KafkaProvider.SELF_MANAGED)));
        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildSchemaResponse("subject-reference")));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.validateSchema(namespace, schema))
                .consumeNextWith(errors -> assertEquals(expectedResult, errors.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldGetEmptySchemaReferences() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("prefix.schema-one-value");
        schema.getSpec().setReferences(Collections.emptyList());

        StepVerifier.create(schemaService.getSchemaReferences(namespace, schema))
                .consumeNextWith(refs -> assertTrue(refs.isEmpty()))
                .verifyComplete();
    }

    @Test
    void shouldGetSchemaReferences() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("prefix.schema-one-value");
        SchemaResponse schemaResponse = buildReferenceSchemaResponse("header-value");
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(schemaResponse));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.getSchemaReferences(namespace, schema))
                .consumeNextWith(refs -> assertTrue(
                        refs.containsKey(schemaResponse.subject()) && refs.containsValue(schemaResponse.schema())))
                .verifyComplete();
    }

    @Test
    void shouldBeEqualByCanonicalStringAndRefs() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("prefix.schema-one-value");
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();
        Schema schemaV2 = buildSchemaV2();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildReferenceSchemaResponse("header-value")));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.existInOldVersions(namespace, schema, List.of(schema, schemaV2)))
                .consumeNextWith(Assertions::assertTrue)
                .verifyComplete();
    }

    @Test
    void shouldBeEqualByCanonicalString() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("prefix.schema-one-value");
        schema.getSpec().setReferences(Collections.emptyList());
        Schema schemaV2 = buildSchemaV2();

        StepVerifier.create(schemaService.existInOldVersions(namespace, schema, List.of(schema, schemaV2)))
                .consumeNextWith(Assertions::assertTrue)
                .verifyComplete();
    }

    @Test
    void shouldNotBeEqualByCanonicalString() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("prefix.schema-one-value");
        schema.getSpec().setReferences(Collections.emptyList());
        Schema schemaV2 = buildSchemaV2();

        StepVerifier.create(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
                .consumeNextWith(Assertions::assertFalse)
                .verifyComplete();
    }

    @Test
    void shouldNotBeEqualByCanonicalStringAndRefs() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("prefix.schema-one-value");
        Schema schemaV2 = buildSchemaV2();
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildReferenceSchemaResponse("header-value")));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
                .consumeNextWith(Assertions::assertFalse)
                .verifyComplete();
    }

    @Test
    void shouldExtractRecordName() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema("com.michelin.kafka.producer.showcase.avro.PersonAvro");
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "header-value", "1"))
                .thenReturn(Mono.just(buildReferenceSchemaResponse("header-value")));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.extractRecordName(namespace, schema))
                .consumeNextWith(result -> assertEquals("com.michelin.kafka.producer.showcase.avro.PersonAvro", result))
                .verifyComplete();
    }

    @Test
    void shouldExtractRecordNameFromUnion() {
        Namespace namespace = buildNamespace();
        Schema schema = Schema.builder()
                .spec(Schema.SchemaSpec.builder()
                        .schema("""
                            {
                             "type": "record",
                             "namespace": "com.michelin.kafka.avro",
                             "name": "AllTypes",
                             "fields": [
                               {
                                 "name": "oneOf",
                                 "type": [
                                   "com.michelin.kafka.avro.Customer",
                                   "com.michelin.kafka.avro.Product",
                                   "com.michelin.kafka.avro.Order"
                                 ]
                               }
                             ]
                            }
                            """)
                        .references(List.of(
                                Schema.SchemaSpec.Reference.builder()
                                        .name("com.michelin.kafka.avro.Customer")
                                        .subject("abc.customer-value")
                                        .version(1)
                                        .build(),
                                Schema.SchemaSpec.Reference.builder()
                                        .name("com.michelin.kafka.avro.Product")
                                        .subject("abc.product-value")
                                        .version(1)
                                        .build(),
                                Schema.SchemaSpec.Reference.builder()
                                        .name("com.michelin.kafka.avro.Order")
                                        .subject("abc.order-value")
                                        .version(1)
                                        .build()))
                        .build())
                .build();

        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();
        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "abc.customer-value", "1"))
                .thenReturn(Mono.just(SchemaResponse.builder()
                        .id(1)
                        .version(1)
                        .subject("abc.customer-value")
                        .schema("{\"namespace\":\"com.michelin.kafka.avro\",\"type\":\"record\","
                                + "\"name\":\"Customer\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"Header ID\"}]}")
                        .build()));
        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "abc.product-value", "1"))
                .thenReturn(Mono.just(SchemaResponse.builder()
                        .id(1)
                        .version(1)
                        .subject("abc.product-value")
                        .schema("{\"namespace\":\"com.michelin.kafka.avro\",\"type\":\"record\","
                                + "\"name\":\"Product\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"Header ID\"}]}")
                        .build()));
        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(), "abc.order-value", "1"))
                .thenReturn(Mono.just(SchemaResponse.builder()
                        .id(1)
                        .version(1)
                        .subject("abc.order-value")
                        .schema("{\"namespace\":\"com.michelin.kafka.avro\",\"type\":\"record\","
                                + "\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"string\"],"
                                + "\"default\":null,\"doc\":\"Header ID\"}]}")
                        .build()));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any()))
                .thenReturn(Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.extractRecordName(namespace, schema))
                .consumeNextWith(result -> assertEquals("com.michelin.kafka.avro.AllTypes", result))
                .verifyComplete();
    }

    @Test
    void shouldNotExtractRecordNameWhenSchemaIsNull() {
        Namespace namespace = buildNamespace();
        Schema schema =
                Schema.builder().spec(Schema.SchemaSpec.builder().build()).build();

        StepVerifier.create(schemaService.extractRecordName(namespace, schema)).verifyComplete();
    }

    @Test
    void shouldNotExtractRecordNameForNonAvroSchemas() {
        Namespace namespace = buildNamespace();
        Schema schema = Schema.builder()
                .spec(Schema.SchemaSpec.builder()
                        .schemaType(Schema.SchemaType.JSON)
                        .build())
                .build();

        StepVerifier.create(schemaService.extractRecordName(namespace, schema)).verifyComplete();
    }

    @Test
    void shouldExtractResourceNameFromSubject() {
        assertEquals("abc-topic-name", schemaService.extractResourceNameFromSubject("abc-topic-name-key"));
        assertEquals("abc.topic-name", schemaService.extractResourceNameFromSubject("abc.topic-name-key"));

        assertEquals("abc-topic-name", schemaService.extractResourceNameFromSubject("abc-topic-name-value"));
        assertEquals("abc.topic-name", schemaService.extractResourceNameFromSubject("abc.topic-name-value"));

        assertEquals(
                "com.michelin.kafka.producer.showcase.avro.PersonAvro",
                schemaService.extractResourceNameFromSubject("com.michelin.kafka.producer.showcase.avro.PersonAvro"));

        assertEquals(
                "abc-topic-name",
                schemaService.extractResourceNameFromSubject(
                        "abc-topic-name-com.michelin.kafka.producer.showcase.avro.PersonAvro"));
        assertEquals(
                "abc.topic-name",
                schemaService.extractResourceNameFromSubject(
                        "abc.topic-name-com.michelin.kafka.producer.showcase.avro.PersonAvro"));
    }

    private static Stream<Arguments> subjectStrategiesForConfluentCloud() {
        Map<String, ResourceValidator.Validator> defaultKeyTopicNameStrategy = Map.of(
                KEY_SUBJECT_NAME_STRATEGY,
                ResourceValidator.ValidString.optionalIn(defaultStrategy().toString()));

        Map<String, ResourceValidator.Validator> defaultValueTopicNameStrategy = Map.of(
                VALUE_SUBJECT_NAME_STRATEGY,
                ResourceValidator.ValidString.optionalIn(defaultStrategy().toString()));

        Map<String, ResourceValidator.Validator> defaultKeyRecordNameStrategy =
                Map.of(KEY_SUBJECT_NAME_STRATEGY, ResourceValidator.ValidString.optionalIn(RECORD_NAME.toString()));

        Map<String, ResourceValidator.Validator> defaultValueRecordNameStrategy =
                Map.of(VALUE_SUBJECT_NAME_STRATEGY, ResourceValidator.ValidString.optionalIn(RECORD_NAME.toString()));

        Map<String, ResourceValidator.Validator> defaultKeyTopicRecordNameStrategy = Map.of(
                KEY_SUBJECT_NAME_STRATEGY, ResourceValidator.ValidString.optionalIn(TOPIC_RECORD_NAME.toString()));

        Map<String, ResourceValidator.Validator> defaultValueTopicRecordNameStrategy = Map.of(
                VALUE_SUBJECT_NAME_STRATEGY, ResourceValidator.ValidString.optionalIn(TOPIC_RECORD_NAME.toString()));

        return Stream.of(
                // Invalid: Missing -key suffix
                Arguments.of(defaultKeyTopicNameStrategy, "abc.topic-name-missingpart", "{\"name\":\"User\"}", false),
                // Invalid: Missing -value suffix
                Arguments.of(defaultValueTopicNameStrategy, "abc.topic-name-missingpart", "{\"name\":\"User\"}", false),
                // Valid: Value strategy set to topic name strategy by default
                Arguments.of(defaultKeyTopicNameStrategy, "abc.topic-name-value", "{\"name\":\"User\"}", true),
                // Valid: Key strategy set to topic name strategy by default
                Arguments.of(defaultValueTopicNameStrategy, "abc.topic-name-key", "{\"name\":\"User\"}", true),
                // Valid: No strategy defined, default to topic name strategy
                Arguments.of(Map.of(), "abc.topic-name-key", "{\"name\":\"User\"}", true),
                // Valid: No strategy defined, default to topic name strategy
                Arguments.of(Map.of(), "abc.topic-name-value", "{\"name\":\"User\"}", true),
                // Valid: Key subject with topic name strategy
                Arguments.of(defaultKeyTopicNameStrategy, "abc.topic-name-key", "{\"name\":\"User\"}", true),
                // Valid: Value subject with topic name strategy
                Arguments.of(defaultValueTopicNameStrategy, "abc.topic-name-value", "{\"name\":\"User\"}", true),
                // Invalid: Wrong strategy for given subject
                Arguments.of(
                        defaultValueTopicNameStrategy,
                        "com.example.User",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        false),
                // Valid: Key subject with record name strategy
                Arguments.of(
                        defaultKeyRecordNameStrategy,
                        "com.example.User",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        true),
                // Valid: Value subject with record name strategy
                Arguments.of(
                        defaultValueRecordNameStrategy,
                        "com.example.User",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        true),
                // Invalid: Wrong strategy for given subject
                Arguments.of(
                        defaultValueRecordNameStrategy,
                        "abc.topic-name-value",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        false),
                // Invalid: Record name does not match subject
                Arguments.of(
                        defaultValueRecordNameStrategy,
                        "org.sample.LoginEvent",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        false),
                // Valid: Key subject with topic record name strategy
                Arguments.of(
                        defaultKeyTopicRecordNameStrategy,
                        "abc.topic-name-com.example.User",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        true),
                // Valid: Value subject with topic record name strategy
                Arguments.of(
                        defaultValueTopicRecordNameStrategy,
                        "abc.topic-name-com.example.User",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        true),
                // Invalid: Wrong strategy for given subject
                Arguments.of(
                        defaultValueTopicRecordNameStrategy,
                        "abc.topic-name-value",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        false),
                // Invalid: Record name does not match subject
                Arguments.of(
                        defaultValueTopicRecordNameStrategy,
                        "abc.topic-name-org.sample.LoginEvent",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        false));
    }

    private static Stream<Arguments> subjectStrategiesForSelfManaged() {
        return Stream.of(
                // Invalid: Missing -key suffix
                Arguments.of("abc.topic-name-missingpart", "{\"name\":\"User\"}", false),
                // Invalid: Missing -value suffix
                Arguments.of("abc.topic-name-missingpart", "{\"name\":\"User\"}", false),
                // Valid: Value strategy set to topic name strategy by default
                Arguments.of("abc.topic-name-value", "{\"name\":\"User\"}", true),
                // Valid: Key strategy set to topic name strategy by default
                Arguments.of("abc.topic-name-key", "{\"name\":\"User\"}", true),
                // Valid: No strategy defined, default to topic name strategy
                Arguments.of("abc.topic-name-key", "{\"name\":\"User\"}", true),
                // Valid: No strategy defined, default to topic name strategy
                Arguments.of("abc.topic-name-value", "{\"name\":\"User\"}", true),
                // Valid: Key subject with topic name strategy
                Arguments.of("abc.topic-name-key", "{\"name\":\"User\"}", true),
                // Valid: Value subject with topic name strategy
                Arguments.of("abc.topic-name-value", "{\"name\":\"User\"}", true),
                // Invalid: Wrong strategy for given subject
                Arguments.of(
                        "com.example.User",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        false),
                // Valid: Key subject with topic record name strategy
                Arguments.of(
                        "abc.topic-name-com.example.User",
                        "{\"name\":\"User\",\"namespace\":\"com.example\",\"type\":\"record\",\"fields\":[]}",
                        false));
    }

    private Namespace buildNamespace() {
        return Namespace.builder()
                .metadata(
                        Metadata.builder().name("myNamespace").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();
    }

    private Schema buildSchema(String subject) {
        return Schema.builder()
                .metadata(Metadata.builder().name(subject).build())
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

    private SchemaCompatibilityResponse buildCompatibilityResponse() {
        return SchemaCompatibilityResponse.builder()
                .compatibilityLevel(Schema.Compatibility.BACKWARD)
                .build();
    }
}
