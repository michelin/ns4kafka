package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaList;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.SchemaService;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.utils.SecurityService;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    ApplicationEventPublisher<?> applicationEventPublisher;

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
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Single.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Maybe.empty());
        when(schemaService.register(namespace, schema)).thenReturn(Single.just(1));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        schemaController.apply("myNamespace", schema, false)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "created"))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getMetadata().getName().equals("prefix.subject-value"));
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
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Single.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName()))
                .thenReturn(Maybe.just(schema));
        when(schemaService.register(namespace, schema)).thenReturn(Single.just(2));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        schemaController.apply("myNamespace", schema, false)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "changed"))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getMetadata().getName().equals("prefix.subject-value"));
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
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Single.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Maybe.just(schema));
        when(schemaService.register(namespace, schema)).thenReturn(Single.just(1));

        schemaController.apply("myNamespace", schema, false)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "unchanged"))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getMetadata().getName().equals("prefix.subject-value"));
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

        schemaController.apply("myNamespace", schema, false)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1L)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Invalid value wrongSubjectName for name: subject must end with -key or -value"));

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

        schemaController.apply("myNamespace", schema, false)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1L)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Namespace not owner of this schema prefix.subject-value."));
    }

    @Test
    void applyDryRunCreated() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Single.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Maybe.empty());

        schemaController.apply("myNamespace", schema, true)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "created"))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getMetadata().getName().equals("prefix.subject-value"));

        verify(schemaService, never()).register(namespace, schema);
    }

    @Test
    void applyDryRunChanged() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(schemaService.isNamespaceOwnerOfSubject(namespace, schema.getMetadata().getName())).thenReturn(true);
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Single.just(List.of()));
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Maybe.just(schema));

        schemaController.apply("myNamespace", schema, true)
                .test()
                .assertValue(response -> Objects.equals(response.header("X-Ns4kafka-Result"), "changed"))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getMetadata().getName().equals("prefix.subject-value"));

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
        when(schemaService.validateSchemaCompatibility("local", schema)).thenReturn(Single.just(List.of("Not compatible")));

        schemaController.apply("myNamespace", schema, true)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1L)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Not compatible"));

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
        when(schemaService.findAllForNamespace(namespace)).thenReturn(Single.just(List.of(schema)));

        schemaController.list("myNamespace")
            .test()
            .assertValue(schemas -> schemas.size() == 1)
            .assertValue(schemas -> schemas.get(0).getMetadata().getName().equals("prefix.subject-value"));
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
        when(schemaService.getLatestSubject(namespace, schema.getMetadata().getName())).thenReturn(Maybe.just(schema));

        schemaController.get("myNamespace", "prefix.subject-value")
                .test()
                .assertValue(response -> response.getMetadata().getName().equals("prefix.subject-value"));
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

        schemaController.get("myNamespace", "prefix.subject-value")
                .test()
                .assertResult();

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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Maybe.empty());

        schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD)
                .test()
                .assertValue(response -> response.getStatus().equals(HttpStatus.NOT_FOUND));

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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Maybe.just(schema));
        when(schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD)).thenReturn(Single.just(SchemaCompatibilityResponse.builder()
                .compatibilityLevel(Schema.Compatibility.FORWARD)
                .build()));

        schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD)
                .test()
                .assertValue(response -> response.getStatus().equals(HttpStatus.OK))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getMetadata().getName().equals("prefix.subject-value")
                        && response.getBody().get().getSpec().getCompatibility().equals(Schema.Compatibility.FORWARD));
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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Maybe.just(schema));

        schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.FORWARD)
                .test()
                .assertValue(response -> response.getStatus().equals(HttpStatus.OK))
                .assertValue(response -> response.getBody().isPresent()
                        && response.getBody().get().getMetadata().getName().equals("prefix.subject-value")
                        && response.getBody().get().getSpec().getCompatibility().equals(Schema.Compatibility.FORWARD));

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

        schemaController.config("myNamespace", "prefix.subject-value", Schema.Compatibility.BACKWARD)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1L)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Invalid prefix prefix.subject-value : namespace not owner of this subject"));

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

        schemaController.deleteSubject("myNamespace", "prefix.subject-value", false)
                .test()
                .assertError(ResourceValidationException.class)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().size() == 1L)
                .assertError(error -> ((ResourceValidationException) error).getValidationErrors().get(0)
                        .equals("Namespace not owner of this schema prefix.subject-value."));

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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Maybe.just(schema));
        when(schemaService.deleteSubject(namespace, "prefix.subject-value")).thenReturn(Single.just(new Integer[1]));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        schemaController.deleteSubject("myNamespace", "prefix.subject-value", false)
                .test()
                .assertValue(response -> response.getStatus().equals(HttpStatus.NO_CONTENT));

        verify(schemaService, times(1)).deleteSubject(namespace, "prefix.subject-value");
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
        when(schemaService.getLatestSubject(namespace, "prefix.subject-value")).thenReturn(Maybe.just(schema));

        schemaController.deleteSubject("myNamespace", "prefix.subject-value", true)
                .test()
                .assertValue(response -> response.getStatus().equals(HttpStatus.NO_CONTENT));

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
