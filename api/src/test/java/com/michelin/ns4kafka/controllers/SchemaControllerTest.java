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

import java.util.Collections;
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
    void apply() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.register(namespace, schema)).thenReturn(Optional.of(schema));

        HttpResponse<Schema> response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject", actual.getMetadata().getName());
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
        Assertions.assertEquals("Invalid prefix prefix.subject : namespace not owner of this subject", exception.getValidationErrors().get(0));
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

        HttpResponse<Schema> response = this.schemaController.apply("myNamespace", schema, true);

        Schema actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject", actual.getMetadata().getName());
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
    void getAllByNamespace() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.getAllByNamespace(namespace)).thenReturn(List.of(schema));

        List<Schema> response = this.schemaController.getAllByNamespace("myNamespace");

        Assertions.assertEquals(1L, response.size());
        Assertions.assertEquals("prefix.subject", response.get(0).getMetadata().getName());
    }

    /**
     * Test to get a subject by namespace and subject
     */
    @Test
    void getByNamespaceAndSubject() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(this.schemaService.getBySubjectAndVersion(namespace, schema.getMetadata().getName(), "latest")).thenReturn(Optional.of(schema));

        Optional<Schema> response = this.schemaController.getByNamespaceAndSubject("myNamespace", "prefix.subject");

        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.isPresent());
        Assertions.assertEquals("prefix.subject", response.get().getMetadata().getName());
    }

    /**
     * Test to get a subject by namespace and subject name when the required subject does not belong to the namespace
     */
    @Test
    void getByNamespaceAndSubjectNamespaceNotOwnerOfSubject() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.getByNamespaceAndSubject("myNamespace", "prefix.subject"));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid prefix prefix.subject : namespace not owner of this subject", exception.getValidationErrors().get(0));
        verify(this.schemaService, never()).getBySubjectAndVersion(namespace, schema.getMetadata().getName(), "latest");
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
        when(this.schemaService.getBySubjectAndVersion(namespace, "prefix.subject", "latest")).thenReturn(Optional.empty());

        HttpResponse<Optional<Schema>> response = this.schemaController.compatibility("myNamespace", "prefix.subject", Map.of("compatibility", Schema.Compatibility.FORWARD), false);

        Optional<Schema> actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertTrue(actual.isEmpty());
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        verify(this.schemaService, never())
                .updateSubjectCompatibility(namespace, "prefix.subject", Schema.Compatibility.FORWARD);
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
        when(this.schemaService.getBySubjectAndVersion(namespace, "prefix.subject", "latest")).thenReturn(Optional.of(schema));
        when(this.schemaService.updateSubjectCompatibility(namespace, "prefix.subject", Schema.Compatibility.FORWARD)).thenReturn(Optional.of(updatedSchema));

        HttpResponse<Optional<Schema>> response = this.schemaController.compatibility("myNamespace", "prefix.subject", Map.of("compatibility", Schema.Compatibility.FORWARD), false);

        Optional<Schema> actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject", actual.get().getMetadata().getName());
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
        when(this.schemaService.getBySubjectAndVersion(namespace, "prefix.subject", "latest")).thenReturn(Optional.of(schema));

        HttpResponse<Optional<Schema>> response = this.schemaController.compatibility("myNamespace", "prefix.subject", Map.of("compatibility", Schema.Compatibility.BACKWARD), false);

        Optional<Schema> actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject", actual.get().getMetadata().getName());
        verify(this.schemaService, never())
                .updateSubjectCompatibility(namespace, "prefix.subject", Schema.Compatibility.BACKWARD);
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
                this.schemaController.compatibility("myNamespace", "prefix.subject", compatibilityMap, false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid prefix prefix.subject : namespace not owner of this subject", exception.getValidationErrors().get(0));
        verify(this.schemaService, never()).updateSubjectCompatibility(namespace, "prefix.subject", Schema.Compatibility.BACKWARD);
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
        when(this.schemaService.getBySubjectAndVersion(namespace, "prefix.subject", "latest")).thenReturn(Optional.of(schema));

        HttpResponse<Optional<Schema>> response = this.schemaController.compatibility("myNamespace", "prefix.subject", Map.of("compatibility", Schema.Compatibility.BACKWARD), true);

        Optional<Schema> actual = response.body();

        Assertions.assertNotNull(actual);
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        Assertions.assertEquals("prefix.subject", actual.get().getMetadata().getName());
        Assertions.assertEquals(Schema.Compatibility.BACKWARD, actual.get().getSpec().getCompatibility());
        verify(this.schemaService, never()).updateSubjectCompatibility(namespace, "prefix.subject", Schema.Compatibility.BACKWARD);
    }

    /**
     * Test the subject deletion when the namespace is not owner of the subject
     */
    @Test
    void deleteSubjectNamespaceNotOwnerOfSubject() {
        Namespace namespace = this.buildNamespace();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject")).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.deleteSubject("myNamespace", "prefix.subject", false));

        Assertions.assertEquals(1L, exception.getValidationErrors().size());
        Assertions.assertEquals("Invalid prefix prefix.subject : namespace not owner of this subject", exception.getValidationErrors().get(0));
        verify(this.schemaService, never()).updateSubjectCompatibility(namespace, "prefix.subject", Schema.Compatibility.BACKWARD);
    }

    /**
     * Test the subject deletion
     */
    @Test
    void deleteSubject() {
        Namespace namespace = this.buildNamespace();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject")).thenReturn(true);
        doNothing().when(this.schemaService).deleteSubject(namespace, "prefix.subject");

        HttpResponse<Void> response = this.schemaController.deleteSubject("myNamespace", "prefix.subject", false);

        Assertions.assertNotNull(response);
        verify(this.schemaService, times(1)).deleteSubject(namespace, "prefix.subject");
    }

    /**
     * Test the subject deletion in dry mode
     */
    @Test
    void deleteSubjectDryRun() {
        Namespace namespace = this.buildNamespace();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSubject(namespace, "prefix.subject")).thenReturn(true);

        HttpResponse<Void> response = this.schemaController.deleteSubject("myNamespace", "prefix.subject", true);

        Assertions.assertNotNull(response);
        verify(this.schemaService, never()).deleteSubject(namespace, "prefix.subject");
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
                        .name("prefix.subject")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .compatibility(Schema.Compatibility.BACKWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();
    }
}
