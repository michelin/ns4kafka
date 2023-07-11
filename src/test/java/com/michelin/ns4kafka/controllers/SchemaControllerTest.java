package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AuditLog;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaList;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.SchemaService;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

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

    /**
     * Test the schema creation
     * The response should contain a "created" header
     */
    @Test
    void applyCreated() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Mono.empty());
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

    /**
     * Test the schema creation
     * The response should contain a "changed" header
     */
    @Test
    void applyChanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName()))
                .thenReturn(Mono.just(schema));
        when(schemaService.register(namespace, schema)).thenReturn(Mono.just(2));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
            .consumeNextWith(response -> {
                assertEquals("changed", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
            })
            .verifyComplete();
    }

    /**
     * Test the schema creation
     * The response should contain an "unchanged" header
     */
    @Test
    void applyUnchanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Mono.just(schema));
        when(schemaService.register(namespace, schema)).thenReturn(Mono.just(1));

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
            .consumeNextWith(response -> {
                assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
            })
            .verifyComplete();
    }

    /**
     * Test the schema creation when the subject has wrong format
     */
    @Test
    void applyWrongSubjectName() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        schema.getMetadata().setName("wrongSubjectName");

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));

        StepVerifier.create(schemaController.apply("myNamespace", schema, false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Invalid value wrongSubjectName for name: subject must end with -key or -value", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();

        verify(schemaService, never()).register(namespace, schema);
    }


    /**
     * Test the schema creation when the subject does not belong to the namespace
     */
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
                assertEquals("Namespace not owner of this schema prefix.subject-value.", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();
    }

    @Test
    void applyDryRunCreated() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Mono.empty());

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

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.apply("myNamespace", schema, true))
            .consumeNextWith(response -> {
                assertEquals("changed", response.header("X-Ns4kafka-Result"));
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
            })
            .verifyComplete();

        verify(schemaService, never()).register(namespace, schema);
    }

    /**
     * Test the schema creation in dry mode when the schema is not compatible
     */
    @Test
    void applyDryRunNotCompatible() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Mono.just(List.of("Not compatible")));

        StepVerifier.create(schemaController.apply("myNamespace", schema, true))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Not compatible", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();

        verify(schemaService, never()).register(namespace, schema);
    }

    /**
     * Test to get all schemas of namespace
     */
    @Test
    void list() {
        Namespace namespace = buildNamespace();
        SchemaList schema = buildSchemaList();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findAllForNamespace(namespace)).thenReturn(Flux.fromIterable(List.of(schema)));

        StepVerifier.create(schemaController.list("myNamespace"))
            .consumeNextWith(schemaResponse -> {
                assertEquals("prefix.subject-value", schemaResponse.getMetadata().getName());
            })
            .verifyComplete();
    }

    /**
     * Test to get a subject by namespace and subject
     */
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

    /**
     * Test to get a subject by namespace and subject name when the required subject does not belong to the namespace
     */
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

    /**
     * Test the compatibility update
     */
    @Test
    void compatibilityUpdateSubjectNotExist() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.empty());

        StepVerifier.create(schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();

        verify(schemaService, never()).updateSubjectCompatibility(any(), any(), any());
    }

    /**
     * Test the compatibility update
     */
    @Test
    void compatibilityUpdateChanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.just(schema));
        when(schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD)).thenReturn(Mono.just(SchemaCompatibilityResponse.builder()
                .compatibilityLevel(Schema.Compatibility.FORWARD)
                .build()));

        StepVerifier.create(schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD))
            .consumeNextWith(response -> {
                assertEquals(HttpStatus.OK, response.getStatus());
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
                assertEquals(Schema.Compatibility.FORWARD, response.getBody().get().getSpec().getCompatibility());
            })
            .verifyComplete();
    }

    /**
     * Test the compatibility update when the compat did not change
     */
    @Test
    void compatibilityUpdateUnchanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        schema.getSpec().setCompatibility(Schema.Compatibility.FORWARD);

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.just(schema));

        StepVerifier.create(schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD))
            .consumeNextWith(response -> {
                assertEquals(HttpStatus.OK, response.getStatus());
                assertTrue(response.getBody().isPresent());
                assertEquals("prefix.subject-value", response.getBody().get().getMetadata().getName());
                assertEquals(Schema.Compatibility.FORWARD, response.getBody().get().getSpec().getCompatibility());
            })
            .verifyComplete();

        verify(schemaService, never()).updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD);
    }

    /**
     * Test the compatibility update when the namespace is not owner of the subject
     */
    @Test
    void compatibilityUpdateNamespaceNotOwnerOfSubject() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(false);

        StepVerifier.create(schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.BACKWARD))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Invalid prefix prefix.subject-value : namespace not owner of this subject", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();

        verify(schemaService, never()).updateSubjectCompatibility(any(), any(), any());
    }

    /**
     * Test the subject deletion when the namespace is not owner of the subject
     */
    @Test
    void deleteSubjectNamespaceNotOwnerOfSubject() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(false);

        StepVerifier.create(schemaController.deleteSubject("myNamespace", "prefix.subject-value", false))
            .consumeErrorWith(error -> {
                assertEquals(ResourceValidationException.class, error.getClass());
                assertEquals(1, ((ResourceValidationException) error).getValidationErrors().size());
                assertEquals("Namespace not owner of this schema prefix.subject-value.", ((ResourceValidationException) error).getValidationErrors().get(0));
            })
            .verify();

        verify(schemaService, never()).updateSubjectCompatibility(any(), any(), any());
    }

    /**
     * Test the subject deletion
     */
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

    /**
     * Should not delete subject when empty
     */
    @Test
    void shouldNotDeleteSubjectWhenEmpty() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(true);
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Mono.empty());

        StepVerifier.create(schemaController.deleteSubject("myNamespace", "prefix.subject-value", false))
                .consumeNextWith(response -> assertEquals(HttpStatus.NOT_FOUND, response.getStatus()))
                .verifyComplete();
    }

    /**
     * Test the subject deletion in dry mode
     */
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

    /**
     * Build a namespace resource
     * @return The namespace
     */
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

    /**
     * Build a schema resource
     * @return The schema
     */
    private Schema buildSchema() {
        return Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject-value")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .id(1)
                        .version(1)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();
    }

    private SchemaList buildSchemaList() {
        return SchemaList.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject-value")
                        .build())
                .build();
    }
}
