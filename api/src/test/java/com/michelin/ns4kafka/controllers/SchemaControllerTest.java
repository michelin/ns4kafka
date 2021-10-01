package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.SchemaService;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibilityCheck;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class SchemaControllerTest {
    /**
     * The schema service
     */
    @Mock
    SchemaService schemaService;

    /**
     * The namespace service
     */
    @Mock
    NamespaceService namespaceService;

    /**
     * The application event publisher
     */
    @Mock
    ApplicationEventPublisher applicationEventPublisher;

    /**
     * The security service
     */
    @Mock
    SecurityService securityService;

    /**
     * The schema controller
     */
    @InjectMocks
    SchemaController schemaController;

    /**
     * Test the schema creation
     */
    @Test
    void create() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(true);
        when(this.schemaService.findByName(namespace, "prefix.schema")).thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(this.applicationEventPublisher).publishEvent(any());

        when(this.schemaService.create(schema)).thenReturn(schema);

        var response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.schema", actual.getMetadata().getName());
    }

    /**
     * Test the schema creation when the schema already exists and did not change
     */
    @Test
    void createAlreadyExistingNoChange() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(true);
        when(this.schemaService.findByName(namespace, "prefix.schema")).thenReturn(Optional.of(schema));

        var response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.schema", actual.getMetadata().getName());
    }

    /**
     * Test the schema creation when the schema already exists but is currently soft deleted
     */
    @Test
    void createAlreadyExistingButSoftDeleted() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema existingSchema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .status(Schema.SchemaStatus.ofSoftDeleted())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(true);
        when(this.schemaService.findByName(namespace, "prefix.schema")).thenReturn(Optional.of(existingSchema));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(this.applicationEventPublisher).publishEvent(any());

        when(this.schemaService.create(schema)).thenReturn(schema);

        var response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.schema", actual.getMetadata().getName());
    }

    /**
     * Test the schema creation when the schema already exists but is currently soft deleted
     */
    @Test
    void createAlreadyExistingButPendingHardDeletion() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema existingSchema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .status(Schema.SchemaStatus.ofPendingHardDeletion())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(true);
        when(this.schemaService.findByName(namespace, "prefix.schema")).thenReturn(Optional.of(existingSchema));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(this.applicationEventPublisher).publishEvent(any());

        when(this.schemaService.create(schema)).thenReturn(schema);

        var response = this.schemaController.apply("myNamespace", schema, false);

        Schema actual = response.body();
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.schema", actual.getMetadata().getName());
    }

    /**
     * Test the schema creation when the schema does not belong to the namespace
     */
    @Test
    void createNotOwnerOfNamespace() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.apply("myNamespace", schema, false));

        assertEquals(1L, exception.getValidationErrors().size());
        assertEquals("Invalid prefix prefix.schema : namespace not owner of this schema", exception.getValidationErrors().get(0));
    }

    /**
     * Test the schema creation when the schema is not compatible with the latest version
     */
    @Test
    void createNotCompatible() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.schemaService.validateSchemaCompatibility("local", schema))
                .thenReturn(List.of("The schema registry rejected the given schema for compatibility reason"));

        when(this.namespaceService.findByName("myNamespace"))
                .thenReturn(Optional.of(namespace));

        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any()))
                .thenReturn(true);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.apply("myNamespace", schema, false));

        assertEquals(1L, exception.getValidationErrors().size());
        assertEquals("The schema registry rejected the given schema for compatibility reason", exception.getValidationErrors().get(0));
    }

    /**
     * Test the schema creation in dry mode
     */
    @Test
    void createDryRun() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(true);
        when(this.schemaService.findByName(namespace, "prefix.schema")).thenReturn(Optional.empty());

        var response = this.schemaController.apply("myNamespace", schema, true);

        Schema actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.schema", actual.getMetadata().getName());
        verify(this.schemaService, never()).create(schema);
    }

    /**
     * Test to get a schema by namespace and subject
     */
    @Test
    void getByNamespaceAndSubject() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(true);
        when(this.schemaService.findByName(any(), any())).thenReturn(Optional.of(schema));

        var response = this.schemaController.getByNamespaceAndSubject("myNamespace", "subject");

        assertEquals("prefix.schema", response.get().getMetadata().getName());
    }

    /**
     * Test to get a schema by namespace and subject when the required schema does not belong to the namespace
     */
    @Test
    void getByNamespaceAndSubjectNotOwnerOfNamespace() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.schemaService.isNamespaceOwnerOfSchema(any(), any())).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.getByNamespaceAndSubject("myNamespace", "subject"));

        assertEquals(1L, exception.getValidationErrors().size());
        assertEquals("Invalid prefix subject : namespace not owner of this schema", exception.getValidationErrors().get(0));
    }

    /**
     * Assert an exception is thrown when trying to delete a schema from another namespace
     */
    @Test
    void deleteSchemaNotOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.schemaService.isNamespaceOwnerOfSchema(ns, "schema"))
                .thenReturn(false);

        Assertions.assertThrows(ResourceValidationException.class, () ->
                this.schemaController.deleteBySubject("namespace", "schema", false, false));
    }

    /**
     * Test the schema soft deletion
     */
    @Test
    void softDeleteConnectorOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        Mockito.when(this.namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.schemaService.isNamespaceOwnerOfSchema(ns, "schema"))
                .thenReturn(true);

        when(this.schemaService.findByName(any(), any()))
                .thenReturn(Optional.of(schema));

        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> response = Assertions.assertDoesNotThrow(() ->
                this.schemaController.deleteBySubject("namespace", "schema", false, false));

        Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }

    /**
     * Test the schema hard deletion
     */
    @Test
    void hardDeleteConnectorOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        Mockito.when(this.namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.schemaService.isNamespaceOwnerOfSchema(ns, "schema"))
                .thenReturn(true);

        when(this.schemaService.findByName(any(), any()))
                .thenReturn(Optional.of(schema));

        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> response = Assertions.assertDoesNotThrow(() ->
                this.schemaController.deleteBySubject("namespace", "schema", false, true));

        Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }


    /**
     * Assert that deleting a non existing schema returns a not found response
     */
    @Test
    void deleteSchemaNotExisting() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.schemaService.isNamespaceOwnerOfSchema(ns, "schema"))
                .thenReturn(true);

        when(this.schemaService.findByName(any(), any()))
                .thenReturn(Optional.empty());

        HttpResponse<Void> response = Assertions.assertDoesNotThrow(() ->
                this.schemaController.deleteBySubject("namespace", "schema", false, false));

        Assertions.assertEquals(HttpStatus.NOT_FOUND, response.getStatus());
    }

    /**
     * Test the schema deletion in dry mode
     */
    @Test
    void deleteSchemaDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Schema schema = Schema.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.schema")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .content(Schema.SchemaSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .status(Schema.SchemaStatus.ofPendingHardDeletion())
                .build();

        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.schemaService.isNamespaceOwnerOfSchema(ns, "schema"))
                .thenReturn(true);

        when(this.schemaService.findByName(any(), any()))
                .thenReturn(Optional.of(schema));

        HttpResponse<Void> response = Assertions.assertDoesNotThrow(() ->
                this.schemaController.deleteBySubject("namespace", "schema", true, false));

        Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getStatus());

        verify(this.schemaService, never()).create(schema);
    }
}
