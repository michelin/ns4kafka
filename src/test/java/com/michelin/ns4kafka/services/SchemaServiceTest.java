package com.michelin.ns4kafka.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaResponse;
import java.util.Arrays;
import java.util.List;
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
    AccessControlEntryService accessControlEntryService;

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Test
    void getAllByNamespace() {
        Namespace namespace = buildNamespace();
        List<String> subjectsResponse =
            Arrays.asList("prefix.schema-one", "prefix2.schema-two", "prefix2.schema-three");

        when(schemaRegistryClient.getSubjects(namespace.getMetadata().getCluster())).thenReturn(
            Flux.fromIterable(subjectsResponse));
        when(accessControlEntryService.findAllGrantedToNamespace(namespace))
            .thenReturn(List.of(
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("prefix.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("prefix2.schema-two")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.READ)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("prefix3.")
                        .build())
                    .build(),
                AccessControlEntry.builder()
                    .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("namespace")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .resource("ns-")
                        .build())
                    .build()
            ));

        StepVerifier.create(schemaService.findAllForNamespace(namespace))
            .consumeNextWith(schema -> assertEquals("prefix.schema-one", schema.getMetadata().getName()))
            .consumeNextWith(schema -> assertEquals("prefix2.schema-two", schema.getMetadata().getName()))
            .verifyComplete();
    }

    @Test
    void getAllByNamespaceEmptyResponse() {
        Namespace namespace = buildNamespace();

        when(schemaRegistryClient.getSubjects(namespace.getMetadata().getCluster())).thenReturn(Flux.empty());

        StepVerifier.create(schemaService.findAllForNamespace(namespace))
            .verifyComplete();
    }

    @Test
    void getBySubjectAndVersion() {
        Namespace namespace = buildNamespace();
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(schemaRegistryClient.getLatestSubject(namespace.getMetadata().getCluster(),
            "prefix.schema-one")).thenReturn(Mono.just(buildSchemaResponse("prefix.schema-one")));
        when(schemaRegistryClient.getCurrentCompatibilityBySubject(any(), any())).thenReturn(
            Mono.just(compatibilityResponse));

        StepVerifier.create(schemaService.getLatestSubject(namespace, "prefix.schema-one"))
            .consumeNextWith(latestSubject -> {
                assertEquals("prefix.schema-one", latestSubject.getMetadata().getName());
                assertEquals("local", latestSubject.getMetadata().getCluster());
                assertEquals("myNamespace", latestSubject.getMetadata().getNamespace());
            })
            .verifyComplete();
    }

    @Test
    void getAllSubjectVersions() {
        Namespace namespace = buildNamespace();
        SchemaResponse schemaResponse = buildSchemaResponse("prefix.schema-one");

        when(schemaRegistryClient.getAllSubjectVersions(namespace.getMetadata().getCluster(),
            "prefix.schema-one")).thenReturn(Flux.just(schemaResponse));

        StepVerifier.create(schemaService.getAllSubjectVersions(namespace, "prefix.schema-one"))
            .consumeNextWith(subjectVersion -> {
                assertEquals("prefix.schema-one", subjectVersion.getMetadata().getName());
                assertEquals("local", subjectVersion.getMetadata().getCluster());
                assertEquals("myNamespace", subjectVersion.getMetadata().getNamespace());
                assertEquals(schemaResponse.references(), subjectVersion.getSpec().getReferences());
            })
            .verifyComplete();
    }

    @Test
    void getBySubjectAndVersionEmptyResponse() {
        Namespace namespace = buildNamespace();

        when(schemaRegistryClient.getLatestSubject(namespace.getMetadata().getCluster(), "prefix.schema-one"))
            .thenReturn(Mono.empty());

        StepVerifier.create(schemaService.getLatestSubject(namespace, "prefix.schema-one"))
            .verifyComplete();
    }

    @Test
    void register() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.register(any(), any(), any()))
            .thenReturn(Mono.just(SchemaResponse.builder().id(1).version(1).build()));

        StepVerifier.create(schemaService.register(namespace, schema))
            .consumeNextWith(id -> assertEquals(1, id))
            .verifyComplete();
    }

    @Test
    void deleteSubject() {
        Namespace namespace = buildNamespace();

        when(schemaRegistryClient.deleteSubject(namespace.getMetadata().getCluster(),
            "prefix.schema-one", false)).thenReturn(Mono.just(new Integer[] {1}));

        when(schemaRegistryClient.deleteSubject(namespace.getMetadata().getCluster(),
            "prefix.schema-one", true)).thenReturn(Mono.just(new Integer[] {1}));

        StepVerifier.create(schemaService.deleteSubject(namespace, "prefix.schema-one"))
            .consumeNextWith(ids -> {
                assertEquals(1, ids.length);
                assertEquals(1, ids[0]);
            })
            .verifyComplete();

        verify(schemaRegistryClient, times(1)).deleteSubject(namespace.getMetadata().getCluster(),
            "prefix.schema-one", false);

        verify(schemaRegistryClient, times(1)).deleteSubject(namespace.getMetadata().getCluster(),
            "prefix.schema-one", true);
    }

    @Test
    void validateSchemaCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SchemaCompatibilityCheckResponse schemaCompatibilityCheckResponse = SchemaCompatibilityCheckResponse.builder()
            .isCompatible(true)
            .build();

        when(schemaRegistryClient.validateSchemaCompatibility(any(), any(), any()))
            .thenReturn(Mono.just(schemaCompatibilityCheckResponse));

        StepVerifier.create(schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema))
            .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
            .verifyComplete();
    }

    @Test
    void invalidateSchemaCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SchemaCompatibilityCheckResponse schemaCompatibilityCheckResponse = SchemaCompatibilityCheckResponse.builder()
            .isCompatible(false)
            .messages(List.of("Incompatible schema"))
            .build();

        when(schemaRegistryClient.validateSchemaCompatibility(any(), any(), any()))
            .thenReturn(Mono.just(schemaCompatibilityCheckResponse));

        StepVerifier.create(schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema))
            .consumeNextWith(errors -> {
                assertEquals(1, errors.size());
                assertTrue(errors.contains("Incompatible schema"));
            })
            .verifyComplete();
    }

    @Test
    void validateSchemaCompatibility404NotFound() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.validateSchemaCompatibility(any(), any(), any()))
            .thenReturn(Mono.empty());

        StepVerifier.create(schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema))
            .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
            .verifyComplete();
    }

    @Test
    void updateSubjectCompatibilityResetToDefault() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.deleteCurrentCompatibilityBySubject(any(), any()))
            .thenReturn(Mono.just(SchemaCompatibilityResponse.builder()
                .compatibilityLevel(Schema.Compatibility.FORWARD)
                .build()));

        StepVerifier.create(schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.GLOBAL))
            .consumeNextWith(schemaCompatibilityResponse -> assertEquals(Schema.Compatibility.FORWARD,
                schemaCompatibilityResponse.compatibilityLevel()))
            .verifyComplete();

        verify(schemaRegistryClient, times(1)).deleteCurrentCompatibilityBySubject(any(), any());
    }

    @Test
    void updateSubjectCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.updateSubjectCompatibility(any(), any(), any()))
            .thenReturn(Mono.just(SchemaCompatibilityResponse.builder()
                .compatibilityLevel(Schema.Compatibility.FORWARD)
                .build()));

        StepVerifier.create(schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD))
            .consumeNextWith(schemaCompatibilityResponse -> assertEquals(Schema.Compatibility.FORWARD,
                schemaCompatibilityResponse.compatibilityLevel()))
            .verifyComplete();

        verify(schemaRegistryClient, times(1)).updateSubjectCompatibility(any(), any(), any());
    }

    @Test
    void isNamespaceOwnerOfSubjectTest() {
        Namespace ns = buildNamespace();
        when(accessControlEntryService.isNamespaceOwnerOfResource("myNamespace", AccessControlEntry.ResourceType.TOPIC,
            "prefix.schema-one"))
            .thenReturn(true);

        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-key"));
        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-value"));
        assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one"));
    }

    @Test
    void shouldValidateSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(),
            "subject-reference", 1))
            .thenReturn(Mono.just(buildSchemaResponse("subject-reference")));

        StepVerifier.create(schemaService.validateSchema(namespace, schema))
            .consumeNextWith(errors -> assertTrue(errors.isEmpty()))
            .verifyComplete();
    }

    @Test
    void shouldNotValidateSchema() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        schema.getMetadata().setName("wrongSubjectName");
        schema.getSpec().getReferences().add(Schema.SchemaSpec.Reference.builder()
            .name("reference")
            .subject("subject-reference")
            .version(1)
            .build());

        when(schemaRegistryClient.getSubject(namespace.getMetadata().getCluster(),
            "subject-reference", 1)).thenReturn(Mono.empty());

        StepVerifier.create(schemaService.validateSchema(namespace, schema))
            .consumeNextWith(errors -> {
                assertTrue(errors.contains("Invalid value wrongSubjectName for name: "
                    + "Value must end with -key or -value."));
                assertTrue(errors.contains("Reference subject-reference with version 1 not found."));
            })
            .verifyComplete();
    }

    private Namespace buildNamespace() {
        return Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();
    }

    private Schema buildSchema() {
        return Schema.builder()
            .metadata(ObjectMeta.builder()
                .name("prefix.schema-one")
                .build())
            .spec(Schema.SchemaSpec.builder()
                .compatibility(Schema.Compatibility.BACKWARD)
                .schema(
                    "{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                        + "\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                        + "\"default\":null,\"doc\":\"First name of the person\"},"
                        + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                        + "\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\","
                        + "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                        + "\"doc\":\"Date of birth of the person\"}]}")
                .build())
            .build();
    }

    private SchemaResponse buildSchemaResponse(String subject) {
        return SchemaResponse.builder()
            .id(1)
            .version(1)
            .subject(subject)
            .schema(
                "{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
                    + "\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],"
                    + "\"default\":null,\"doc\":\"First name of the person\"},"
                    + "{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,"
                    + "\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\","
                    + "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,"
                    + "\"doc\":\"Date of birth of the person\"}]}")
            .references(List.of(Schema.SchemaSpec.Reference.builder()
                .name("reference")
                .subject("subject-reference")
                .version(1)
                .build()))
            .build();
    }

    private SchemaCompatibilityResponse buildCompatibilityResponse() {
        return SchemaCompatibilityResponse.builder()
            .compatibilityLevel(Schema.Compatibility.BACKWARD)
            .build();
    }
}
