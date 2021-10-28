package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaResponse;
import io.micronaut.http.HttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

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
        SchemaCompatibilityResponse compatibilityResponse = this.buildCompatibilityResponse();

        when(accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(),
                AccessControlEntry.ResourceType.SCHEMA, "prefix.schema-one")).thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(),
                AccessControlEntry.ResourceType.SCHEMA, "prefix2.schema-two")).thenReturn(true);
        when(accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(),
                AccessControlEntry.ResourceType.SCHEMA, "prefix2.schema-three")).thenReturn(false);
        when(kafkaSchemaRegistryClient.getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())).thenReturn(HttpResponse.ok(subjectsResponse));
        when(kafkaSchemaRegistryClient.getSchemaBySubjectAndVersion(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), "prefix.schema-one", "latest")).thenReturn(HttpResponse.ok(this.buildSchemaResponse("prefix.schema-one")));
        when(kafkaSchemaRegistryClient.getSchemaBySubjectAndVersion(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), "prefix2.schema-two", "latest")).thenReturn(HttpResponse.ok(this.buildSchemaResponse("prefix2.schema-two")));
        when(kafkaSchemaRegistryClient.getCurrentCompatibilityBySubject(any(), any(), any())).thenReturn(HttpResponse.ok(compatibilityResponse));

        List<Schema> retrievedSchemas = this.schemaService.getAllByNamespace(namespace);
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

        when(kafkaSchemaRegistryClient.getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())).thenReturn(HttpResponse.ok());

        List<Schema> retrievedSchemas = this.schemaService.getAllByNamespace(namespace);
        Assertions.assertTrue(retrievedSchemas.isEmpty());
    }

    /**
     * Tests to find all schemas by namespace
     */
    @Test
    void getBySubjectAndVersion() {
        Namespace namespace = this.buildNamespace();
        SchemaCompatibilityResponse compatibilityResponse = this.buildCompatibilityResponse();

        when(kafkaSchemaRegistryClient.getSchemaBySubjectAndVersion(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), "prefix.schema-one", "latest")).thenReturn(HttpResponse.ok(this.buildSchemaResponse("prefix.schema-one")));
        when(kafkaSchemaRegistryClient.getCurrentCompatibilityBySubject(any(), any(), any())).thenReturn(HttpResponse.ok(compatibilityResponse));

        Optional<Schema> retrievedSchema = this.schemaService.getBySubjectAndVersion(namespace, "prefix.schema-one", "latest");

        Assertions.assertTrue(retrievedSchema.isPresent());
        Assertions.assertEquals("prefix.schema-one", retrievedSchema.get().getMetadata().getName());
    }

    /**
     * Tests to register a new schema to the schema registry
     */
    @Test
    void register() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();
        SchemaResponse schemaResponse = this.buildSchemaResponse("prefix.schema-one");
        SchemaCompatibilityResponse compatibilityResponse = this.buildCompatibilityResponse();

        when(kafkaSchemaRegistryClient.register(any(), any(), any(), any()))
                .thenReturn(HttpResponse.ok(schemaResponse));
        when(kafkaSchemaRegistryClient.getSchemaBySubjectAndVersion(any(), any(), any(), any()))
                .thenReturn(HttpResponse.ok(schemaResponse));
        when(kafkaSchemaRegistryClient.getCurrentCompatibilityBySubject(any(), any(), any())).thenReturn(HttpResponse.ok(compatibilityResponse));

        Optional<Schema> retrievedSchema = this.schemaService.register(namespace, schema);

        Assertions.assertTrue(retrievedSchema.isPresent());
        Assertions.assertEquals("prefix.schema-one", retrievedSchema.get().getMetadata().getName());
    }

    /**
     * Tests to register a new schema to the schema registry, and the response is empty
     */
    @Test
    void registerEmptyBody() {
        Namespace namespace = this.buildNamespace();
        Schema schema = this.buildSchema();

        when(kafkaSchemaRegistryClient.register(any(), any(), any(), any()))
                .thenReturn(HttpResponse.ok());

        Optional<Schema> retrievedSchema = this.schemaService.register(namespace, schema);

        Assertions.assertTrue(retrievedSchema.isEmpty());
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
                .thenReturn(HttpResponse.ok(schemaCompatibilityCheckResponse));

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
                .messages(Collections.singletonList("Incompatible schema"))
                .build();

        when(kafkaSchemaRegistryClient.validateSchemaCompatibility(any(), any(), any(), any()))
                .thenReturn(HttpResponse.ok(schemaCompatibilityCheckResponse));

        List<String> validationResponse = this.schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema);

        Assertions.assertEquals(1L, validationResponse.size());
        Assertions.assertTrue(validationResponse.contains("Incompatible schema"));
    }

    /**
     * Tests the schema compatibility validation
     */
    @Test
    void updateSubjectCompatibility() {
        Namespace namespace = this.buildNamespace();
        SchemaResponse schemaResponse = this.buildSchemaResponse("prefix.schema-one");
        SchemaCompatibilityResponse compatibilityResponse = this.buildCompatibilityResponse();

        doNothing().when(kafkaSchemaRegistryClient).updateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                "prefix.schema-one", Collections.singletonMap("compatibility", Schema.Compatibility.FORWARD.toString()));
        when(kafkaSchemaRegistryClient.getSchemaBySubjectAndVersion(any(), any(), any(), any()))
                .thenReturn(HttpResponse.ok(schemaResponse));
        when(kafkaSchemaRegistryClient.getCurrentCompatibilityBySubject(any(), any(), any())).thenReturn(HttpResponse.ok(compatibilityResponse));

        Optional<Schema> updatedSchema = this.schemaService
                .updateSubjectCompatibility(namespace, "prefix.schema-one", Schema.Compatibility.FORWARD);

        Assertions.assertTrue(updatedSchema.isPresent());
        Assertions.assertEquals("prefix.schema-one", updatedSchema.get().getMetadata().getName());
        verify(kafkaSchemaRegistryClient, times(1)).updateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                "prefix.schema-one", Collections.singletonMap("compatibility", Schema.Compatibility.FORWARD.toString()));
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
                        .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
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
                .schema("{\"namespace\":\"com.michelin.kafka.producer.showcase.avro\",\"type\":\"record\",\"name\":\"PersonAvro\",\"fields\":[{\"name\":\"firstName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"First name of the person\"},{\"name\":\"lastName\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last name of the person\"},{\"name\":\"dateOfBirth\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null,\"doc\":\"Date of birth of the personnn\"}]}")
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
