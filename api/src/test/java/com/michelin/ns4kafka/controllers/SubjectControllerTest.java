package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.NamespaceService;
import com.michelin.ns4kafka.services.SubjectService;
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
class SubjectControllerTest {
    /**
     * The subject service
     */
    @Mock
    SubjectService subjectService;

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
     * The subject controller
     */
    @InjectMocks
    SubjectController subjectController;

    /**
     * Test the subject creation
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

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.subjectService.validateSubjectCompatibility("local", subject)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.subjectService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(true);
        when(this.subjectService.findByName(namespace, "prefix.subject")).thenReturn(Optional.empty());
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(this.applicationEventPublisher).publishEvent(any());

        when(this.subjectService.create(subject)).thenReturn(subject);

        var response = this.subjectController.apply("myNamespace", subject, false);

        Subject actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.subject", actual.getMetadata().getName());
    }

    /**
     * Test the subject creation when the subject already exists and did not change
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

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.subjectService.validateSubjectCompatibility("local", subject)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.subjectService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(true);
        when(this.subjectService.findByName(namespace, "prefix.subject")).thenReturn(Optional.of(subject));

        var response = this.subjectController.apply("myNamespace", subject, false);

        Subject actual = response.body();
        Assertions.assertEquals("unchanged", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.subject", actual.getMetadata().getName());
    }

    /**
     * Test the subject creation when the subject already exists but is currently in deletion
     */
    @Test
    void createAlreadyExistingButInDeletion() {
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("myNamespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .topicValidator(TopicValidator.makeDefault())
                        .build())
                .build();

        Subject existingSubject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .finalizer("to_delete")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.subjectService.validateSubjectCompatibility("local", subject)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.subjectService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(true);
        when(this.subjectService.findByName(namespace, "prefix.subject")).thenReturn(Optional.of(existingSubject));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(this.applicationEventPublisher).publishEvent(any());

        when(this.subjectService.create(subject)).thenReturn(subject);

        var response = this.subjectController.apply("myNamespace", subject, false);

        Subject actual = response.body();
        Assertions.assertEquals("changed", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.subject", actual.getMetadata().getName());
    }

    /**
     * Test the subject creation when the subject does not belong to the namespace
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

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.subjectService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.subjectController.apply("myNamespace", subject, false));

        assertEquals(1L, exception.getValidationErrors().size());
        assertEquals("Invalid prefix prefix.subject : namespace not owner of this subject", exception.getValidationErrors().get(0));
    }

    /**
     * Test the subject creation when the subject is not compatible with the latest version
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

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.subjectService.validateSubjectCompatibility("local", subject))
                .thenReturn(List.of("The schema registry rejected the given subject for compatibility reason"));

        when(this.namespaceService.findByName("myNamespace"))
                .thenReturn(Optional.of(namespace));

        when(this.subjectService.isNamespaceOwnerOfSubject(any(), any()))
                .thenReturn(true);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.subjectController.apply("myNamespace", subject, false));

        assertEquals(1L, exception.getValidationErrors().size());
        assertEquals("The schema registry rejected the given subject for compatibility reason", exception.getValidationErrors().get(0));
    }

    /**
     * Test the subject creation in dry mode
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

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.subjectService.validateSubjectCompatibility("local", subject)).thenReturn(Collections.emptyList());
        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.subjectService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(true);
        when(this.subjectService.findByName(namespace, "prefix.subject")).thenReturn(Optional.empty());

        var response = this.subjectController.apply("myNamespace", subject, true);

        Subject actual = response.body();
        Assertions.assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertEquals("prefix.subject", actual.getMetadata().getName());
        verify(this.subjectService, never()).create(subject);
    }

    /**
     * Test to get a subject by namespace and subject
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

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        when(this.namespaceService.findByName("myNamespace")).thenReturn(Optional.of(namespace));
        when(this.subjectService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(true);
        when(this.subjectService.findByName(any(), any())).thenReturn(Optional.of(subject));

        var response = this.subjectController.getByNamespaceAndSubject("myNamespace", "subject");

        assertEquals("prefix.subject", response.get().getMetadata().getName());
    }

    /**
     * Test to get a subject by namespace and subject name when the required subject does not belong to the namespace
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
        when(this.subjectService.isNamespaceOwnerOfSubject(any(), any())).thenReturn(false);

        ResourceValidationException exception = Assertions.assertThrows(ResourceValidationException.class, () ->
                this.subjectController.getByNamespaceAndSubject("myNamespace", "subject"));

        assertEquals(1L, exception.getValidationErrors().size());
        assertEquals("Invalid prefix subject : namespace not owner of this subject", exception.getValidationErrors().get(0));
    }

    /**
     * Assert an exception is thrown when trying to delete a subject from another namespace
     */
    @Test
    void deleteSubjectNotOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.subjectService.isNamespaceOwnerOfSubject(ns, "subject"))
                .thenReturn(false);

        Assertions.assertThrows(ResourceValidationException.class, () ->
                this.subjectController.deleteBySubject("namespace", "subject", false));
    }

    /**
     * Test the subject deletion
     */
    @Test
    void deleteSubjectOwned() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        Mockito.when(this.namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.subjectService.isNamespaceOwnerOfSubject(ns, "subject"))
                .thenReturn(true);

        when(this.subjectService.findByName(any(), any()))
                .thenReturn(Optional.of(subject));

        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole(ResourceBasedSecurityRule.IS_ADMIN)).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        HttpResponse<Void> response = Assertions.assertDoesNotThrow(() ->
                this.subjectController.deleteBySubject("namespace", "subject", false));

        Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getStatus());
    }

    /**
     * Assert that deleting a non existing subject returns a not found response
     */
    @Test
    void deleteSubjectNotExisting() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.subjectService.isNamespaceOwnerOfSubject(ns, "subject"))
                .thenReturn(true);

        when(this.subjectService.findByName(any(), any()))
                .thenReturn(Optional.empty());

        HttpResponse<Void> response = Assertions.assertDoesNotThrow(() ->
                this.subjectController.deleteBySubject("namespace", "subject", false));

        Assertions.assertEquals(HttpStatus.NOT_FOUND, response.getStatus());
    }

    /**
     * Test the subject deletion in dry mode
     */
    @Test
    void deleteSubjectDryRun() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .build();

        Subject subject = Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name("prefix.subject")
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
                                .build())
                        .build())
                .build();

        Mockito.when(namespaceService.findByName("namespace"))
                .thenReturn(Optional.of(ns));

        Mockito.when(this.subjectService.isNamespaceOwnerOfSubject(ns, "subject"))
                .thenReturn(true);

        when(this.subjectService.findByName(any(), any()))
                .thenReturn(Optional.of(subject));

        HttpResponse<Void> response = Assertions.assertDoesNotThrow(() ->
                this.subjectController.deleteBySubject("namespace", "subject", true));

        Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getStatus());

        verify(this.subjectService, never()).delete(subject);
    }
}
