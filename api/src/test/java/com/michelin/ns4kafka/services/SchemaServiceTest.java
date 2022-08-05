package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaResponse;
import io.reactivex.Maybe;
import io.reactivex.Single;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;

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
     * Test to find all schemas by namespace
     */
    @Test
    void getAllByNamespace() {
        Namespace namespace = buildNamespace();
        List<String> subjectsResponse = Arrays.asList("prefix.schema-one", "prefix2.schema-two", "prefix2.schema-three");

        when(kafkaSchemaRegistryClient.getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())).thenReturn(Single.just(subjectsResponse));
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

        schemaService.findAllForNamespace(namespace)
            .test()
            .assertValue(schemas -> schemas.size() == 2)
            .assertValue(schemas -> schemas.get(0).getMetadata().getName().equals("prefix.schema-one"))
            .assertValue(schemas -> schemas.get(1).getMetadata().getName().equals("prefix2.schema-two"))
            .assertValue(schemas -> schemas.stream().noneMatch(schema -> schema.getMetadata().getName().equals("prefix2.schema-three")));
    }

    /**
     * Test to find all schemas by namespace and the response from the schema registry is empty
     */
    @Test
    void getAllByNamespaceEmptyResponse() {
        Namespace namespace = buildNamespace();

        when(kafkaSchemaRegistryClient.getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())).thenReturn(Single.just(List.of()));

       schemaService.findAllForNamespace(namespace)
                .test()
                .assertValue(List::isEmpty);
    }

    /**
     * Test to find all schemas by namespace
     */
    @Test
    void getBySubjectAndVersion() {
        Namespace namespace = buildNamespace();
        SchemaCompatibilityResponse compatibilityResponse = buildCompatibilityResponse();

        when(kafkaSchemaRegistryClient.getLatestSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), "prefix.schema-one")).thenReturn(Maybe.just(buildSchemaResponse("prefix.schema-one")));
        when(kafkaSchemaRegistryClient.getCurrentCompatibilityBySubject(any(), any(), any())).thenReturn(Maybe.just(compatibilityResponse));

        schemaService.getLatestSubject(namespace, "prefix.schema-one")
                .test()
                .assertValue(latestSubject -> latestSubject.getMetadata().getName().equals("prefix.schema-one"))
                .assertValue(latestSubject -> latestSubject.getMetadata().getCluster().equals("local"))
                .assertValue(latestSubject -> latestSubject.getMetadata().getNamespace().equals("myNamespace"));
    }

    /**
     * Test to find all schemas by namespace
     */
    @Test
    void getBySubjectAndVersionEmptyResponse() {
        Namespace namespace = buildNamespace();

        when(kafkaSchemaRegistryClient.getLatestSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), "prefix.schema-one")).thenReturn(Maybe.empty());

        schemaService.getLatestSubject(namespace, "prefix.schema-one")
                .test()
                .assertResult();
    }


    /**
     * Test to register a new schema to the schema registry
     */
    @Test
    void register() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(kafkaSchemaRegistryClient.register(any(), any(), any(), any()))
                .thenReturn(Single.just(SchemaResponse.builder().id(1).version(1).build()));

        schemaService.register(namespace, schema)
                .test()
                .assertValue(id -> id.equals(1));
    }

    /**
     * Test to delete a subject
     */
    @Test
    void deleteSubject() {
        Namespace namespace = buildNamespace();

        when(kafkaSchemaRegistryClient.deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                "prefix.schema-one", false)).thenReturn(Single.just(new Integer[]{1}));

        when(kafkaSchemaRegistryClient.deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                "prefix.schema-one", true)).thenReturn(Single.just(new Integer[]{1}));

        schemaService.deleteSubject(namespace, "prefix.schema-one")
                .test()
                .assertValue(ids -> ids.length == 1 && ids[0] == 1);

        verify(kafkaSchemaRegistryClient, times(1)).deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                namespace.getMetadata().getCluster(), "prefix.schema-one", false);

        verify(kafkaSchemaRegistryClient, times(1)).deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                namespace.getMetadata().getCluster(), "prefix.schema-one", true);
    }

    /**
     * Test the schema compatibility validation
     */
    @Test
    void validateSchemaCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SchemaCompatibilityCheckResponse schemaCompatibilityCheckResponse = SchemaCompatibilityCheckResponse.builder()
                .isCompatible(true)
                .build();

        when(kafkaSchemaRegistryClient.validateSchemaCompatibility(any(), any(), any(), any()))
                .thenReturn(Maybe.just(schemaCompatibilityCheckResponse));

        schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema)
                .test()
                .assertValue(List::isEmpty);
    }

    /**
     * Test the schema compatibility invalidation
     */
    @Test
    void invalidateSchemaCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();
        SchemaCompatibilityCheckResponse schemaCompatibilityCheckResponse = SchemaCompatibilityCheckResponse.builder()
                .isCompatible(false)
                .messages(List.of("Incompatible schema"))
                .build();

        when(kafkaSchemaRegistryClient.validateSchemaCompatibility(any(), any(), any(), any()))
                .thenReturn(Maybe.just(schemaCompatibilityCheckResponse));

        schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema)
                .test()
                .assertValue(validationErrors -> validationErrors.size() == 1)
                .assertValue(validationErrors -> validationErrors.contains("Incompatible schema"));
    }

    /**
     * Test the schema compatibility validation when the Schema Registry returns 404 not found
     */
    @Test
    void validateSchemaCompatibility404NotFound() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(kafkaSchemaRegistryClient.validateSchemaCompatibility(any(), any(), any(), any()))
                .thenReturn(Maybe.empty());

        schemaService.validateSchemaCompatibility(namespace.getMetadata().getCluster(), schema)
                .test()
                .assertValue(List::isEmpty);
    }

    /**
     * Test the schema compatibility update when reset to default is asked
     */
    @Test
    void updateSubjectCompatibilityResetToDefault() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(kafkaSchemaRegistryClient.deleteCurrentCompatibilityBySubject(any(), any(), any()))
                .thenReturn(Single.just(SchemaCompatibilityResponse.builder()
                        .compatibilityLevel(Schema.Compatibility.FORWARD)
                        .build()));

        schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.GLOBAL)
                .test()
                .assertValue(schemaCompatibilityResponse -> schemaCompatibilityResponse.compatibilityLevel().equals(Schema.Compatibility.FORWARD));

        verify(kafkaSchemaRegistryClient, times(1)).deleteCurrentCompatibilityBySubject(any(), any(), any());
    }

    /**
     * Test the schema compatibility validation
     */
    @Test
    void updateSubjectCompatibility() {
        Namespace namespace = buildNamespace();
        Schema schema = buildSchema();

        when(kafkaSchemaRegistryClient.updateSubjectCompatibility(any(), any(), any(), any()))
                .thenReturn(Single.just(SchemaCompatibilityResponse.builder()
                         .compatibilityLevel(Schema.Compatibility.FORWARD)
                        .build()));

        schemaService.updateSubjectCompatibility(namespace, schema, Schema.Compatibility.FORWARD)
                .test()
                .assertValue(schemaCompatibilityResponse -> schemaCompatibilityResponse.compatibilityLevel().equals(Schema.Compatibility.FORWARD));

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
     * @return The compatibility response
     */
    private SchemaCompatibilityResponse buildCompatibilityResponse() {
        return SchemaCompatibilityResponse.builder()
                .compatibilityLevel(Schema.Compatibility.BACKWARD)
                .build();
    }
}
