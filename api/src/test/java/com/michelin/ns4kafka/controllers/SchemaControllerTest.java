package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.SchemaService;
import io.micronaut.http.HttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
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
     * Test the schema creation
     */
    @Test
    void applyCreated() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of());
        when(this.schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Optional.empty());
        when(this.schemaService.register(namespace, schema)).thenReturn(Optional.of(schema));

        HttpResponse<Schema> response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
    }

    /**
     * Test the schema creation
     */
    @Test
    void applyChanged() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of());
        when(this.schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Optional.of(schema));
        when(this.schemaService.register(namespace, schema)).thenReturn(Optional.of(schema));

        HttpResponse<Schema> response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
    }

    /**
     * Test the schema creation
     */
    @Test
    void applyWrongSubjectName() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();
        schema.getMetadata().setName("wrongSubjectName");

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.apply("myNamespace", schema, false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid value wrongSubjectName for name: : subject must end with -key or -value", exception.getValidationErrors().get(0));
        verify(this.schemaService, never()).register(namespace, schema);
    }


    /**
     * Test the schema creation when the subject does not belong to the namespace
     */
    @Test
    void applyNamespaceNotOwnerOfSubject() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.apply("myNamespace", schema, false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid value prefix.subject-value for name: : namespace not OWNER of underlying topic", exception.getValidationErrors().get(0));
        verify(this.schemaService, never()).register(namespace, schema);
    }

    /**
     * Test the schema creation in dry mode
     */
    @Test
    void applyDryRun() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of());
        when(this.schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Optional.empty());

        HttpResponse<Schema> response = this.schemaController.apply("myNamespace", schema, true);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.getMetadata().getName());
        verify(this.schemaService, never()).register(namespace, schema);
    }

    /**
     * Test the schema creation in dry mode when the schema is not compatible
     */
    @Test
    void applyDryRunNotCompatible() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(List.of("Not compatible"));

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.apply("myNamespace", schema, true));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Not compatible", exception.getValidationErrors().get(0));
        verify(this.schemaService, never()).register(namespace, schema);
    }

    /**
     * Test to get all schemas of namespace
     */
    @Test
    void list() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.findAllForNamespace(namespace)).thenReturn(List.of(schema));

        List<Schema> response = this.schemaController.list("myNamespace");

        Assertions.assertEquals(1L, response.size());
        Assertions.assertEquals("prefix.subject-value", response.get(0).getMetadata().getName());
    }

    /**
     * Test to get a subject by namespace and subject
     */
    @Test
    void get() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Optional.of(schema));

        Optional<Schema> response = this.schemaController.get("myNamespace", "prefix.subject-value");

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isPresent());
        Assertions.assertEquals("prefix.subject-value", response.get().getMetadata().getName());
    }

    /**
     * Test to get a subject by namespace and subject name when the required subject does not belong to the namespace
     */
    @Test
    void getNamespaceNotOwnerOfSubject() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(false);

        Optional<Schema> response = this.schemaController.get("myNamespace", "prefix.subject-value");

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isEmpty());
        verify(this.schemaService, never()).getLatestSubject(namespace, schema.getMetadata().getName());
    }

    /**
     * Test the compatibility update
     */
    @Test
    void compatibilityUpdateSubjectNotExist() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Optional.empty());

        HttpResponse<Optional<Schema>> response = this.schemaController.compatibility("myNamespace", "prefix.subject-value", Map.of("compatibility", Schema.Compatibility.FORWARD), false);

        Optional<Schema> actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertTrue(actual.isEmpty());
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        verify(this.schemaService, never())
                .updateSubjectCompatibility(namespace, "prefix.subject-value", Schema.Compatibility.FORWARD);
    }

    /**
     * Test the compatibility update
     */
    @Test
    void compatibilityUpdateChanged() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();
        Schema updatedSchema = this.buildSchema();
        updatedSchema.getSpec().setCompatibility(Schema.Compatibility.FORWARD);

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Optional.of(schema));
        when(this.schemaService.updateSubjectCompatibility(namespace, "prefix.subject-value", Schema.Compatibility.FORWARD)).thenReturn(Optional.of(updatedSchema));

        HttpResponse<Optional<Schema>> response = this.schemaController.compatibility("myNamespace", "prefix.subject-value", Map.of("compatibility", Schema.Compatibility.FORWARD), false);

        Optional<Schema> actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.get().getMetadata().getName());
        Assertions.assertEquals(Schema.Compatibility.FORWARD, actual.get().getSpec().getCompatibility());
    }

    /**
     * Test the compatibility update when the compatibility mode to apply is still the same
     */
    @Test
    void compatibilityUpdateUnchanged() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Optional.of(schema));

        HttpResponse<Optional<Schema>> response = this.schemaController.compatibility("myNamespace", "prefix.subject-value", Map.of("compatibility", Schema.Compatibility.BACKWARD), false);

        Optional<Schema> actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.get().getMetadata().getName());
        verify(this.schemaService, never())
                .updateSubjectCompatibility(namespace, "prefix.subject-value", Schema.Compatibility.BACKWARD);
    }

    /**
     * Test the compatibility update when the namespace is not owner of the subject
     */
    @Test
    void compatibilityUpdateNamespaceNotOwnerOfSubject() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();
        Map<String, Schema.Compatibility> compatibilityMap = Map.of("compatibility", Schema.Compatibility.BACKWARD);

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.compatibility("myNamespace", "prefix.subject-value", compatibilityMap, false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid prefix prefix.subject-value : namespace not owner of this subject", exception.getValidationErrors().get(0));
        verify(this.schemaService, never()).updateSubjectCompatibility(namespace, "prefix.subject-value", Schema.Compatibility.BACKWARD);
    }

    /**
     * Test the compatibility update in dry mode
     */
    @Test
    void compatibilityUpdateDryRun() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Optional.of(schema));

        HttpResponse<Optional<Schema>> response = this.schemaController.compatibility("myNamespace", "prefix.subject-value", Map.of("compatibility", Schema.Compatibility.BACKWARD), true);

        Optional<Schema> actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject-value", actual.get().getMetadata().getName());
        Assertions.assertEquals(Schema.Compatibility.BACKWARD, actual.get().getSpec().getCompatibility());
        verify(this.schemaService, never()).updateSubjectCompatibility(namespace, "prefix.subject-value", Schema.Compatibility.BACKWARD);
    }

    /**
     * Test the subject deletion when the namespace is not owner of the subject
     */
    @Test
    void deleteSubjectNamespaceNotOwnerOfSubject() {
        Namespace namespace = this.buildNamespace();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.deleteSubject("myNamespace", "prefix.subject-value", false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid value prefix.subject-value for name: : namespace not OWNER of underlying topic", exception.getValidationErrors().get(0));
        verify(this.schemaService, never()).updateSubjectCompatibility(namespace, "prefix.subject-value", Schema.Compatibility.BACKWARD);
    }

    /**
     * Test the subject deletion
     */
    @Test
    void deleteSubject() {
        Namespace namespace = this.buildNamespace();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(true);
        doNothing().when(this.schemaService).deleteSubject(namespace, "prefix.subject-value");

        HttpResponse<Void> response = this.schemaController.deleteSubject("myNamespace", "prefix.subject-value", false);

        Assertions.assertNotNull(response);
        verify(this.schemaService, times(1)).deleteSubject(namespace, "prefix.subject-value");
    }

    /**
     * Test the subject deletion in dry mode
     */
    @Test
    void deleteSubjectDryRun() {
        Namespace namespace = this.buildNamespace();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject-value")).thenReturn(true);

        HttpResponse<Void> response = this.schemaController.deleteSubject("myNamespace", "prefix.subject-value", true);

        Assertions.assertNotNull(response);
        verify(this.schemaService, never()).deleteSubject(namespace, "prefix.subject-value");
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
                        .compatibility(Schema.Compatibility.BACKWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();
    }
}
