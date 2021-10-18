package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.repositories.SubjectRepository;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityCheckResponse;
import io.micronaut.http.HttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
class KafkaSubjectServiceTest {
    /**
     * The schema service
     */
    @InjectMocks
    SubjectService subjectService;

    /**
     * The ACL service
     */
    @Mock
    AccessControlEntryService accessControlEntryService;

    /**
     * The schema repository
     */
    @Mock
    SubjectRepository subjectRepository;

    /**
     * Kafka schema registry client
     */
    @Mock
    KafkaSchemaRegistryClient kafkaSchemaRegistryClient;

    /**
     * Tests to find a schema by name
     */
    @Test
    void findByName() {
        // Create namespace
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // Create schemas
        Subject mockedSubject1 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Subject mockedSubject2 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-two").build())
                .build();

        Subject mockedSubject3 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-three").build())
                .build();

        // Set schemas to cluster
        Mockito.when(this.subjectRepository.findAllForCluster("local"))
                .thenReturn(List.of(mockedSubject1, mockedSubject2, mockedSubject3));

        Mockito.when(this.accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        // ACL for 1st schema, matching by prefix
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.SUBJECT)
                                        .resource("prefix.")
                                        .build())
                                .build(),
                        // ACL for 2nd schema, matching literally
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.SUBJECT)
                                        .resource("prefix2.schema-two")
                                        .build())
                                .build()
                ));

        // Should succeed, access granted by prefix
        Optional<Subject> schema = this.subjectService.findByName(namespace, "prefix.schema-one");
        Assertions.assertEquals(mockedSubject1, schema.get());

        // Should succeed, access granted literally
        Optional<Subject> schema2 = this.subjectService.findByName(namespace, "prefix2.schema-two");
        Assertions.assertEquals(mockedSubject2, schema2.get());

        // Should fail, access not granted
        Optional<Subject> schemaNotFound = this.subjectService.findByName(namespace, "prefix2.schema-three");
        Assertions.assertThrows(NoSuchElementException.class, () ->
                schemaNotFound.get(), "No value present");
    }

    /**
     * Tests to find all schemas by namespace
     */
    @Test
    void findAllForNamespace() {
        // Create namespace
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // Create schemas
        Subject mockedSubject1 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Subject mockedSubject2 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-two").build())
                .build();

        Subject mockedSubject3 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-three").build())
                .build();

        // Set schemas to cluster
        Mockito.when(this.subjectRepository.findAllForCluster("local"))
                .thenReturn(List.of(mockedSubject1, mockedSubject2, mockedSubject3));

        Mockito.when(this.accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        // ACL for 1st schema, matching by prefix
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.SUBJECT)
                                        .resource("prefix.")
                                        .build())
                                .build(),
                        // ACL for 2nd schema, matching literally
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.SUBJECT)
                                        .resource("prefix2.schema-two")
                                        .build())
                                .build()
                ));

        // Should succeed, access granted by prefix
        List<Subject> subjects = this.subjectService.findAllForNamespace(namespace);
        Assertions.assertEquals(2L, subjects.size());

        Assertions.assertEquals(mockedSubject1, subjects.get(0));

        Assertions.assertEquals(mockedSubject2, subjects.get(1));
    }

    /**
     * Tests to find all schemas by namespace when there is no schema
     */
    @Test
    void findAllForNamespaceNoSchema() {
        // Create namespace
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // Set schemas to cluster
        Mockito.when(this.subjectRepository.findAllForCluster("local"))
                .thenReturn(Collections.emptyList());

        Mockito.when(this.accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        // ACL for 1st schema, matching by prefix
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.SUBJECT)
                                        .resource("prefix.")
                                        .build())
                                .build(),
                        // ACL for 2nd schema, matching literally
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.SUBJECT)
                                        .resource("prefix2.schema-two")
                                        .build())
                                .build()
                ));

        // Should succeed, access granted by prefix
        List<Subject> subjects = this.subjectService.findAllForNamespace(namespace);
        Assertions.assertTrue(subjects.isEmpty());
    }

    /**
     * Tests to find all schemas by namespace when there is no ACL
     */
    @Test
    void findAllForNamespaceNoACL() {
        // Create namespace
        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        // Create schemas
        Subject mockedSubject1 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Subject mockedSubject2 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-two").build())
                .build();

        Subject mockedSubject3 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-three").build())
                .build();

        // Set schemas to cluster
        Mockito.when(this.subjectRepository.findAllForCluster("local"))
                .thenReturn(List.of(mockedSubject1, mockedSubject2, mockedSubject3));

        Mockito.when(this.accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(Collections.emptyList());

        // Should succeed, access granted by prefix
        List<Subject> subjects = this.subjectService.findAllForNamespace(namespace);
        Assertions.assertTrue(subjects.isEmpty());
    }

    /**
     * Tests to create a schema
     */
    @Test
    void create() {
        // Create schemas
        Subject mockedSubject1 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Mockito.when(this.subjectRepository.create(mockedSubject1))
                .thenReturn(mockedSubject1);

        Subject retrievedSubject = this.subjectService.publish(mockedSubject1);
        Assertions.assertEquals(mockedSubject1, retrievedSubject);
    }

    /**
     * Tests the response of the schema compatibility
     */
    @Test
    void verifyCompatibility() {
        // Create schemas
        Subject mockedSubject1 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("schema")
                                .build())
                        .build())
                .build();

        SubjectCompatibilityCheckResponse subjectCompatibilityCheckResponse = SubjectCompatibilityCheckResponse.builder()
                .isCompatible(true)
                .build();

        Mockito.when(this.kafkaSchemaRegistryClient.validateSubjectCompatibility(any(), any(), any(), any()))
                .thenReturn(HttpResponse.ok(subjectCompatibilityCheckResponse));

        List<String> errors = this.subjectService.validateSubjectCompatibility("local", mockedSubject1);

        Assertions.assertTrue(errors.isEmpty());
    }

    /**
     * Tests the response of the schema compatibility when the schema is not compatible
     */
    @Test
    void verifyCompatibilityNotCompatible() {
        // Create schemas
        Subject mockedSubject1 = Subject.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema("schema")
                                .build())
                        .build())
                .build();

        SubjectCompatibilityCheckResponse subjectCompatibilityCheckResponse = SubjectCompatibilityCheckResponse.builder()
                .isCompatible(false)
                .build();

        Mockito.when(this.kafkaSchemaRegistryClient.validateSubjectCompatibility(any(), any(), any(), any()))
                .thenReturn(HttpResponse.ok(subjectCompatibilityCheckResponse));

        List<String> errors = this.subjectService.validateSubjectCompatibility("local", mockedSubject1);

        Assertions.assertEquals(1L, errors.size());
        Assertions.assertEquals("The schema registry rejected the given subject for compatibility reason", errors.get(0));
    }
}
