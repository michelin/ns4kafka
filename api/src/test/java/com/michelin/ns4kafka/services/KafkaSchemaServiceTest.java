package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.repositories.SchemaRepository;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibility;
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
class KafkaSchemaServiceTest {
    /**
     * The schema service
     */
    @InjectMocks
    SchemaService schemaService;

    /**
     * The ACL service
     */
    @Mock
    AccessControlEntryService accessControlEntryService;

    /**
     * The schema repository
     */
    @Mock
    SchemaRepository schemaRepository;

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
        Schema mockedSchema1 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Schema mockedSchema2 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-two").build())
                .build();

        Schema mockedSchema3 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-three").build())
                .build();

        // Set schemas to cluster
        Mockito.when(this.schemaRepository.findAllForCluster("local"))
                .thenReturn(List.of(mockedSchema1, mockedSchema2, mockedSchema3));

        Mockito.when(this.accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        // ACL for 1st schema, matching by prefix
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.SCHEMA)
                                        .resource("prefix.")
                                        .build())
                                .build(),
                        // ACL for 2nd schema, matching literally
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.SCHEMA)
                                        .resource("prefix2.schema-two")
                                        .build())
                                .build()
                ));

        // Should succeed, access granted by prefix
        Optional<Schema> schema = this.schemaService.findByName(namespace, "prefix.schema-one");
        Assertions.assertEquals(mockedSchema1, schema.get());

        // Should succeed, access granted literally
        Optional<Schema> schema2 = this.schemaService.findByName(namespace, "prefix2.schema-two");
        Assertions.assertEquals(mockedSchema2, schema2.get());

        // Should fail, access not granted
        Optional<Schema> schemaNotFound = this.schemaService.findByName(namespace, "prefix2.schema-three");
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
        Schema mockedSchema1 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Schema mockedSchema2 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-two").build())
                .build();

        Schema mockedSchema3 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-three").build())
                .build();

        // Set schemas to cluster
        Mockito.when(this.schemaRepository.findAllForCluster("local"))
                .thenReturn(List.of(mockedSchema1, mockedSchema2, mockedSchema3));

        Mockito.when(this.accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        // ACL for 1st schema, matching by prefix
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.SCHEMA)
                                        .resource("prefix.")
                                        .build())
                                .build(),
                        // ACL for 2nd schema, matching literally
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.SCHEMA)
                                        .resource("prefix2.schema-two")
                                        .build())
                                .build()
                ));

        // Should succeed, access granted by prefix
        List<Schema> schemas = this.schemaService.findAllForNamespace(namespace);
        Assertions.assertEquals(2L, schemas.size());

        Assertions.assertEquals(mockedSchema1, schemas.get(0));

        Assertions.assertEquals(mockedSchema2, schemas.get(1));
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
        Mockito.when(this.schemaRepository.findAllForCluster("local"))
                .thenReturn(Collections.emptyList());

        Mockito.when(this.accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        // ACL for 1st schema, matching by prefix
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.SCHEMA)
                                        .resource("prefix.")
                                        .build())
                                .build(),
                        // ACL for 2nd schema, matching literally
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.SCHEMA)
                                        .resource("prefix2.schema-two")
                                        .build())
                                .build()
                ));

        // Should succeed, access granted by prefix
        List<Schema> schemas = this.schemaService.findAllForNamespace(namespace);
        Assertions.assertTrue(schemas.isEmpty());
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
        Schema mockedSchema1 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Schema mockedSchema2 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-two").build())
                .build();

        Schema mockedSchema3 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix2.schema-three").build())
                .build();

        // Set schemas to cluster
        Mockito.when(this.schemaRepository.findAllForCluster("local"))
                .thenReturn(List.of(mockedSchema1, mockedSchema2, mockedSchema3));

        Mockito.when(this.accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(Collections.emptyList());

        // Should succeed, access granted by prefix
        List<Schema> schemas = this.schemaService.findAllForNamespace(namespace);
        Assertions.assertTrue(schemas.isEmpty());
    }

    /**
     * Tests to create a schema
     */
    @Test
    void create() {
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
        Schema mockedSchema1 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Mockito.when(this.schemaRepository.create(mockedSchema1))
                .thenReturn(mockedSchema1);

        Schema retrievedSchema = this.schemaService.create(mockedSchema1);
        Assertions.assertEquals(mockedSchema1, retrievedSchema);
    }

    /**
     * Tests the response of the schema compatibility
     */
    @Test
    void verifyCompatibility() {
        // Create schemas
        Schema mockedSchema1 = Schema.builder()
                .metadata(ObjectMeta.builder().name("prefix.schema-one").build())
                .build();

        Mockito.when(this.kafkaSchemaRegistryClient.compatibility(any(), any(), any(), any()))
                .thenReturn(HttpResponse.ok(SchemaCompatibility.builder().isCompatible(true).build()));

        SchemaCompatibility schemaCompatibility = this.schemaService.validateSchemaCompatibility("local",mockedSchema1);
        Assertions.assertTrue(schemaCompatibility.isCompatible());
    }
}
