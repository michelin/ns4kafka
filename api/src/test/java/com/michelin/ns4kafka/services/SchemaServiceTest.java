package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SchemaServiceTest {
    /**
     * The schema service
     */
    @InjectMocks
    SchemaService schemaService;

    /**
     * The schema service
     */
    @Mock
    AccessControlEntryService accessControlEntryService;

    /**
     * Kafka schema registry client
     */
    @Mock
    KafkaSchemaRegistryClient kafkaSchemaRegistryClient;

    /**
     * Tests to find all schemas by namespace
     */
    @Test
    void getAllByNamespace() {
        Namespace namespace = this.buildNamespace();
        List<String> subjectsResponse = Arrays.asList("prefix.schema-one", "prefix2.schema-two", "prefix2.schema-three");

        when(kafkaSchemaRegistryClient.getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())).thenReturn(subjectsResponse);
        Mockito.when(accessControlEntryService.findAllGrantedToNamespace(namespace))
                .thenReturn(List.of(
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("prefix.")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("prefix2.schema-two")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.READ)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                        .resource("prefix3.")
                                        .build())
                                .build(),
                        AccessControlEntry.builder()
                                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                        .permission(AccessControlEntry.Permission.OWNER)
                                        .grantedTo("namespace")
                                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                                        .resource("ns-")
                                        .build())
                                .build()
                ));

        List<Schema> retrievedSchemas = this.schemaService.findAllForNamespace(namespace);
        Assertions.assertEquals(2L, retrievedSchemas.size());
        Assertions.assertEquals("prefix.schema-one", retrievedSchemas.get(0).getMetadata().getName());
        Assertions.assertEquals("prefix2.schema-two", retrievedSchemas.get(1).getMetadata().getName());
        Assertions.assertTrue(retrievedSchemas.stream().noneMatch(schema -> schema.getMetadata().getName().equals("prefix2.schema-three")));
    }

    /**
     * Tests to find all schemas by namespace and the response from the schema registry is empty
     */
    @Test
    void getAllByNamespaceEmptyResponse() {
        Namespace namespace = this.buildNamespace();

        when(kafkaSchemaRegistryClient.getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())).thenReturn(List.of());

        List<Schema> retrievedSchemas = this.schemaService.findAllForNamespace(namespace);
        Assertions.assertTrue(retrievedSchemas.isEmpty());
    }

    /**
     * Tests to find all schemas by namespace
     */
    @Test
    void getBySubjectAndVersion() {
        Namespace namespace = this.buildNamespace();
        SchemaCompatibilityResponse compatibilityResponse = this.buildCompatibilityResponse();

        when(kafkaSchemaRegistryClient.getLatestSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), "prefix.schema-one")).thenReturn(Optional.of(this.buildSchemaResponse("prefix.schema-one")));
        when(kafkaSchemaRegistryClient.getCurrentCompatibilityBySubject(any(), any(), any())).thenReturn(Optional.of(compatibilityResponse));

        Optional<Schema> retrievedSchema = this.schemaService.getLatestSubject(namespace, "prefix.schema-one");

        Assertions.assertTrue(retrievedSchema.isPresent());
        Assertions.assertEquals("prefix.schema-one", retrievedSchema.get().getMetadata().getName());
        Assertions.assertEquals("local", retrievedSchema.get().getMetadata().getCluster());
        Assertions.assertEquals("myNamespace", retrievedSchema.get().getMetadata().getNamespace());
    }

    /**
     * Tests to find all schemas by namespace
     */
    @Test
    void getBySubjectAndVersionEmptyResponse() {
        Namespace namespace = this.buildNamespace();

        when(kafkaSchemaRegistryClient.getLatestSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), "prefix.schema-one")).thenReturn(Optional.empty());

        Optional<Schema> retrievedSchema = this.schemaService.getLatestSubject(namespace, "prefix.schema-one");

        Assertions.assertTrue(retrievedSchema.isEmpty());
    }


    /**
     * Tests to register a new schema to the schema registry
     */
    @Test
    void register() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(kafkaSchemaRegistryClient.register(any(), any(), any(), any()))
                .thenReturn(SchemaResponse.builder().id(1).version(1).build());

        Integer retrievedSchemaVersion = this.schemaService.register(namespace, schema);

        Assertions.assertEquals(1, retrievedSchemaVersion);
    }

    /**
     * Tests to delete a subject
     */
    @Test
    void deleteSubject() {
        Namespace namespace = this.buildNamespace();

        doNothing().when(kafkaSchemaRegistryClient).deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                "prefix.schema-one", false);

        doNothing().when(kafkaSchemaRegistryClient).deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                "prefix.schema-one", true);

        this.schemaService.deleteSubject(namespace, "prefix.schema-one");

        verify(kafkaSchemaRegistryClient, times(1)).deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                namespace.getMetadata().getCluster(), "prefix.schema-one", false);

        verify(kafkaSchemaRegistryClient, times(1)).deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                namespace.getMetadata().getCluster(), "prefix.schema-one", true);
    }

    /**
     * Tests the schema compatibility validation
     */
    @Test
    void validateSchemaCompatibility() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();
        SchemaCompatibilityCheckResponse schemaCompatibilityCheckResponse = SchemaCompatibilityCheckResponse.builder()
                .isCompatible(true)
                .build();

        when(kafkaSchemaRegistryClient.validateSchemaCompatibility(any(), any(), any(), any()))
                .thenReturn(Optional.of(schemaCompatibilityCheckResponse));

        List<String> validationResponse = this.schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema);

        Assertions.assertTrue(validationResponse.isEmpty());
    }

    /**
     * Tests the schema compatibility invalidation
     */
    @Test
    void invalidateSchemaCompatibility() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();
        SchemaCompatibilityCheckResponse schemaCompatibilityCheckResponse = SchemaCompatibilityCheckResponse.builder()
                .isCompatible(false)
                .messages(List.of("Incompatible schema"))
                .build();

        when(kafkaSchemaRegistryClient.validateSchemaCompatibility(any(), any(), any(), any()))
                .thenReturn(Optional.of(schemaCompatibilityCheckResponse));

        List<String> validationResponse = this.schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema);

        Assertions.assertEquals(1L, validationResponse.size());
        Assertions.assertTrue(validationResponse.contains("Incompatible schema"));
    }

    /**
     * Tests the schema compatibility validation when the Schema Registry returns 404 not found
     */
    @Test
    void validateSchemaCompatibility404NotFound() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(kafkaSchemaRegistryClient.validateSchemaCompatibility(any(), any(), any(), any()))
                .thenReturn(Optional.empty());

        List<String> validationResponse = this.schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema);

        Assertions.assertTrue(validationResponse.isEmpty());
    }

    /**
     * Tests the schema compatibility validation when the Schema Registry throws an exception
     */
    @Test
    void validateSchemaCompatibilityThrowsException() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(kafkaSchemaRegistryClient.validateSchemaCompatibility(any(), any(), any(), any()))
                .thenThrow(new HttpClientResponseException("Error", HttpResponse.notFound()));

        List<String> validationResponse = this.schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema);

        Assertions.assertEquals(1L, validationResponse.size());
        Assertions.assertEquals("An error occurred during the schema validation (status code: NOT_FOUND)", validationResponse.get(0));
    }

    /**
     * Tests the schema compatibility update when reset to default is asked
     */
    @Test
    void updateSubjectCompatibilityResetToDefault() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        doNothing().when(kafkaSchemaRegistryClient).deleteCurrentCompatibilityBySubject(any(), any(), any());

        this.schemaService
                .updateSubjectCompatibility(namespace, schema, Schema.Compatibility.GLOBAL);

        verify(kafkaSchemaRegistryClient, times(1)).deleteCurrentCompatibilityBySubject(any(), any(), any());
    }

    /**
     * Tests the schema compatibility validation
     */
    @Test
    void updateSubjectCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(kafkaSchemaRegistryClient.updateSubjectCompatibility(any(), any(), any(), any()))
                .thenReturn(compatibilityResponse);

        schemaService
                .updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD);

        verify(kafkaSchemaRegistryClient, times(1)).updateSubjectCompatibility(any(), any(), any(), any());
    }

    /**
     * Test subjects belong to a namespace
     * Assert the "-key"/"-value" suffixes are not taken in account when comparing subjects against the topics ACLs
     */
    @Test
    void isNamespaceOwnerOfSubjectTest() {
        Namespace ns = buildNamespace();
        when(accessControlEntryService.isNamespaceOwnerOfResource("myNamespace", AccessControlEntry.ResourceType.TOPIC, "prefix.schema-one"))
                .thenReturn(true);

        Assertions.assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-key"));
        Assertions.assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one-value"));
        Assertions.assertTrue(schemaService.isNamespaceOwnerOfSubject(ns, "prefix.schema-one"));
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
                        .name("prefix.schema-one")
                        .build())
                .spec(Schema.SchemaSpec.builder()
                        .compatibility(Schema.Compatibility.BACKWARD)
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                        .build())
                .build();
    }

    /**
     * Build a schema response
     *
     * @param subject The subject to set to the schema
     * @return The schema response
     */
    private SchemaResponse buildSchemaResponse(String subject) {
        return SchemaResponse.builder()
                .id(1)
                .version(1)
                .subject(subject)
                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the person\"}]}")
                .build();
    }

    /**
     * Build a schema compatibility response
     *
     * @return The compatibility response
     */
    private SchemaCompatibilityResponse buildCompatibilityResponse() {
        return SchemaCompatibilityResponse.builder()
                .compatibilityLevel(Schema.Compatibility.BACKWARD)
                .build();
    }
}
