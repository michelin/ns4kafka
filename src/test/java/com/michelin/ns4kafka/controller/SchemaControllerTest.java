package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SchemaList;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.SchemaService;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
    void applyCreated() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchema(namespace, schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schema.getMetadata().getName())).thenReturn(Flux.empty());
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
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
            })
            .verifyComplete();
    }

    @Test
    void applyChanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        Schema schemaV2 = buildSchemaV2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchema(namespace, schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.validateSchemaCompatibility("local", schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schemaV2.getMetadata().getName()))
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
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
            })
            .verifyComplete();
    }

    @Test
    void applyUnchanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchema(namespace, schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schema.getMetadata().getName()))
            .thenReturn(Flux.just(schema));
        when(schemaService.existInOldVersions(namespace, schema, List.of(schema)))
            .thenReturn(Mono.just(true));

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
            .consumeNextWith(response -> {
                assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
            })
            .verifyComplete();
    }


    @Test
    void applyNamespaceNotOwnerOfSubject() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(false);

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Invalid value \"prefix.subject-value\" for field \"name\": "
                        + "namespace is not owner of the resource.",
                    ((ResourceValidationException) error).getValidationErrors().getFirst());
            })
            .verify();
    }

    @Test
    void applyValidationErrors() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchema(namespace, schema)).thenReturn(Mono.just(List.of("Errors")));

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Errors",
                    ((ResourceValidationException) error).getValidationErrors().getFirst());
            })
            .verify();
    }

    @Test
    void applyDryRunCreated() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchema(namespace, schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schema.getMetadata().getName())).thenReturn(Flux.empty());
        when(schemaService.existInOldVersions(namespace, schema, Collections.emptyList()))
            .thenReturn(Mono.just(false));

        StepVerifier.create(schemaController.apply("myNamespace", schema, true))
            .consumeNextWith(response -> {
                assertEquals("created", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
            })
            .verifyComplete();

        verify(schemaService, never()).register(namespace, schema);
    }

    @Test
    void applyDryRunChanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        Schema schemaV2 = buildSchemaV2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName()))
            .thenReturn(true);
        when(schemaService.validateSchema(namespace, schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.validateSchemaCompatibility("local", schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schemaV2.getMetadata().getName()))
            .thenReturn(Flux.just(schema));
        when(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
            .thenReturn(Mono.just(false));

        StepVerifier.create(schemaController.apply("myNamespace", schemaV2, true))
            .consumeNextWith(response -> {
                assertEquals("changed", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
            })
            .verifyComplete();

        verify(schemaService, never()).register(namespace, schemaV2);
    }

    @Test
    void applyDryRunNotCompatible() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        Schema schemaV2 = buildSchemaV2();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schemaV2.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchema(namespace, schemaV2)).thenReturn(Mono.just(List.of()));
        when(schemaService.getAllSubjectVersions(namespace, schemaV2.getMetadata().getName()))
            .thenReturn(Flux.just(schema));
        when(schemaService.existInOldVersions(namespace, schemaV2, List.of(schema)))
            .thenReturn(Mono.just(false));
        when(schemaService.validateSchemaCompatibility("local", schemaV2)).thenReturn(
            Mono.just(List.of("Not compatible")));

        StepVerifier.create(schemaController.apply("myNamespace", schemaV2, true))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Not compatible", ((ResourceValidationException) error).getValidationErrors().getFirst());
            })
            .verify();

        verify(schemaService, never()).register(namespace, schema);
    }

    @Test
    void listWithoutParameter() {
        Namespace namespace = buildNamespace();
        SchemaList schema = buildSchemaList();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findAllForNamespace(namespace, "*")).thenReturn(Flux.fromIterable(List.of(schema)));

        StepVerifier.create(schemaController.list("myNamespace", "*"))
            .consumeNextWith(
                schemaResponse -> assertEquals("prefix.subject-value", schemaResponse.getMetadata().getName()))
            .verifyComplete();
    }

    @Test
    void listWithNameParameter() {
        Namespace namespace = buildNamespace();
        SchemaList schema = buildSchemaList();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findAllForNamespace(namespace, "prefix.subject-value"))
            .thenReturn(Flux.fromIterable(List.of(schema)));

        StepVerifier.create(schemaController.list("myNamespace", "prefix.subject-value"))
            .consumeNextWith(
                schemaResponse -> assertEquals("prefix.subject-value", schemaResponse.getMetadata().getName()))
            .verifyComplete();
    }

    @Test
    void get() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.get("myNamespace", "prefix.subject-value"))
            .consumeNextWith(response -> assertEquals("prefix.subject-value", response.getMetadata().getName()))
            .verifyComplete();
    }

    @Test
    void getNamespaceNotOwnerOfSubject() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(false);

        StepVerifier.create(schemaController.get("myNamespace", "prefix.subject-value"))
            .verifyComplete();

        verify(schemaService, never()).getLatestSubject(namespace, schema.getMetadata().getName());
    }

    @Test
    void compatibilityUpdateSubjectNotExist() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.empty());

        StepVerifier.create(
                schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD))
            .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
            .verifyComplete();

        verify(schemaService, never()).updateSubjectCompatibility(any(), any(), any());
    }

    @Test
    void compatibilityUpdateChanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.just(schema));
        when(schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD)).thenReturn(
            Mono.just(SchemaCompatibilityResponse.builder()
                .compatibilityLevel(Schema.Compatibility.FORWARD)
                .build()));

        StepVerifier.create(
                schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD))
            .consumeNextWith(response -> {
                assertEquals(HttpStatus.OK, response.getStatus());
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
                assertEquals(Schema.Compatibility.FORWARD, response.getBody().get().getSpec().getCompatibility());
            })
            .verifyComplete();
    }

    @Test
    void compatibilityUpdateUnchanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        schema.getSpec().setCompatibility(Schema.Compatibility.FORWARD);

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.just(schema));

        StepVerifier.create(
                schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD))
            .consumeNextWith(response -> {
                assertEquals(HttpStatus.OK, response.getStatus());
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
                assertEquals(Schema.Compatibility.FORWARD, response.getBody().get().getSpec().getCompatibility());
            })
            .verifyComplete();

        verify(schemaService, never()).updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD);
    }

    @Test
    void compatibilityUpdateNamespaceNotOwnerOfSubject() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(false);

        StepVerifier.create(
                schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.BACKWARD))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Invalid value \"prefix.subject-value\" for field \"name\": "
                        + "namespace is not owner of the resource.",
                    ((ResourceValidationException) error).getValidationErrors().getFirst());
            })
            .verify();

        verify(schemaService, never()).updateSubjectCompatibility(any(), any(), any());
    }

    @Test
    void deleteSubjectNamespaceNotOwnerOfSubject() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(false);

        StepVerifier.create(schemaController.deleteSubject("myNamespace", "prefix.subject-value", false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Invalid value \"prefix.subject-value\" for field \"name\": "
                        + "namespace is not owner of the resource.",
                    ((ResourceValidationException) error).getValidationErrors().getFirst());
            })
            .verify();

        verify(schemaService, never()).updateSubjectCompatibility(any(), any(), any());
    }

    @Test
    void deleteSubject() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.just(schema));
        when(schemaService.deleteSubject(namespace, "prefix.subject-value")).thenReturn(Mono.just(new Integer[1]));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        StepVerifier.create(schemaController.deleteSubject("myNamespace", "prefix.subject-value", false))
            .consumeNextWith(response -> assertEquals(HttpStatus.NO_CONTENT, response.getStatus()))
            .verifyComplete();

        verify(schemaService, times(1)).deleteSubject(namespace, "prefix.subject-value");
    }

    @Test
    void shouldNotDeleteSubjectWhenEmpty() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.empty());

        StepVerifier.create(schemaController.deleteSubject("myNamespace", "prefix.subject-value", false))
            .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
            .verifyComplete();
    }

    @Test
    void deleteSubjectDryRun() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.deleteSubject("myNamespace", "prefix.subject-value", true))
            .consumeNextWith(response -> assertEquals(HttpStatus.NO_CONTENT, response.getStatus()))
            .verifyComplete();

        verify(schemaService, never()).deleteSubject(namespace, "prefix.subject-value");
    }

    private Namespace buildNamespace() {
        return Namespace.builder()
            .metadata(Metadata.builder()
                .name("myNamespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .build())
            .build();
    }

    private Schema buildSchema() {
        return Schema.builder()
            .metadata(Metadata.builder()
                .name("prefix.subject-value")
                .build())
            .spec(Schema.SchemaSpec.builder()
                .id(1)
                .version(1)
                .schema(
                    "{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
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
            .metadata(Metadata.builder()
                .name("prefix.subject-value")
                .build())
            .spec(Schema.SchemaSpec.builder()
                .id(1)
                .version(2)
                .schema(
                    "{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\","
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

    private SchemaList buildSchemaList() {
        return SchemaList.builder()
            .metadata(Metadata.builder()
                .name("prefix.subject-value")
                .build())
            .build();
    }
}
