package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaCompatibilityState;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.SchemaService;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SchemaControllerTest {
    /**
     * The namespace service
     */
    @Mock
    NamespaceService namespaceService;

    /**
     * The schema service
     */
    @Mock
    SchemaService schemaService;

    /**
     * The schema controller
     */
    @InjectMocks
    SchemaController schemaController;

    /**
     * The security service
     */
    @Mock
    SecurityService securityService;

    /**
     * The app publisher
     */
    @Mock
    ApplicationEventPublisher applicationEventPublisher;

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
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of());
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName()))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(schema));
        when(schemaService.register(namespace, schema)).thenReturn(1);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Schema> response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
    }

    /**
     * Test the schema creation
     * The response should contain a "changed" header
     */
    @Test
    void applyChanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        Schema schemaV2 = buildSchema();
        schemaV2.getSpec().setVersion(2);

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of());
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName()))
                .thenReturn(Optional.of(schema))
                .thenReturn(Optional.of(schemaV2));
        when(schemaService.register(namespace, schema)).thenReturn(2);
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Schema> response = schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
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
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of());
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName()))
                .thenReturn(Optional.of(schema))
                .thenReturn(Optional.of(schema));
        when(schemaService.register(namespace, schema)).thenReturn(1);

        HttpResponse<Schema> response = schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
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

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                schemaController.apply("myNamespace", schema, false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid value wrongSubjectName for name: subject must end with -key or -value", exception.getValidationErrors().get(0));
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

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                schemaController.apply("myNamespace", schema, false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid value prefix.subject-value for name: namespace not OWNER of underlying topic", exception.getValidationErrors().get(0));
        verify(schemaService, never()).register(namespace, schema);
    }

    /**
     * Test the schema creation in dry mode
     */
    @Test
    void applyDryRun() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of());

        HttpResponse<Schema> response = schemaController.apply("myNamespace", schema, true);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertNull(response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
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
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of("Not compatible"));

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                schemaController.apply("myNamespace", schema, true));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Not compatible", exception.getValidationErrors().get(0));
        verify(schemaService, never()).register(namespace, schema);
    }

    /**
     * Test to get all schemas of namespace
     */
    @Test
    void list() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.findAllForNamespace(namespace)).thenReturn(List.of(schema));

        List<Schema> response = schemaController.list("myNamespace");

        Assertions.assertEquals(1L, response.size());
        Assertions.assertEquals("prefix.subject-value", response.get(0).getMetadata().getName());
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
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Optional.of(schema));

        Optional<Schema> response = schemaController.get("myNamespace", "prefix.subject-value");

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isPresent());
        Assertions.assertEquals("prefix.subject-value", response.get().getMetadata().getName());
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

        Optional<Schema> response = schemaController.get("myNamespace", "prefix.subject-value");

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isEmpty());
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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Optional.empty());

        HttpResponse<SchemaCompatibilityState> response = schemaController
                .config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD);

        Assertions.assertEquals(HttpStatus.NOT_FOUND, response.getStatus());
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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Optional.of(schema));
        doNothing().when(schemaService).updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD);

        HttpResponse<SchemaCompatibilityState> response = schemaController
                .config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD);

        SchemaCompatibilityState actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals(HttpStatus.OK, response.getStatus());
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
        Assertions.assertEquals(Schema.Compatibility.FORWARD, actual.getSpec().getCompatibility());
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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Optional.of(schema));

        HttpResponse<SchemaCompatibilityState> response = schemaController
                .config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD);

        SchemaCompatibilityState actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals(HttpStatus.OK, response.getStatus());
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
        Assertions.assertEquals(Schema.Compatibility.FORWARD, actual.getSpec().getCompatibility());
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

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.BACKWARD));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid prefix prefix.subject-value : namespace not owner of this subject", exception.getValidationErrors().get(0));
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

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                schemaController.deleteSubject("myNamespace", "prefix.subject-value", false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid value prefix.subject-value for name: namespace not OWNER of underlying topic", exception.getValidationErrors().get(0));
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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Optional.of(schema));
        doNothing().when(schemaService).deleteSubject(namespace, "prefix.subject-value");
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> response = schemaController.deleteSubject("myNamespace", "prefix.subject-value", false);

        Assertions.assertNotNull(response);
        verify(schemaService, times(1)).deleteSubject(namespace, "prefix.subject-value");
    }

    /**
     * Test the subject deletion in dry mode
     */
    @Test
    void deleteSubjectDryRun() {
        Namespace namespace = buildNamespace();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(true);

        HttpResponse<Void> response = schemaController.deleteSubject("myNamespace", "prefix.subject-value", true);

        Assertions.assertNotNull(response);
        verify(schemaService, never()).deleteSubject(namespace, "prefix.subject-value");
    }

    /**
     * Build a namespace resource
     *
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
     *
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
}
