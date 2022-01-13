package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.client.entities.*;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class SchemaService {
    /**
     * ACLs service
     */
    @Inject
    AccessControlEntryService accessControlEntryService;

    /**
     * Schema Registry client
     */
    @Inject
    KafkaSchemaRegistryClient kafkaSchemaRegistryClient;

    /**
     * Get all the schemas by namespace
     *
     * @param namespace The namespace
     * @return A list of schemas
     */
    public List<Schema> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace).stream()
                .filter(acl -> acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(acl -> acl.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC)
                .collect(Collectors.toList());

        return kafkaSchemaRegistryClient
                .getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())
                .stream()
                .filter(subject -> {
                    String underlyingTopicName = subject.replaceAll("(-key|-value)$","");

                    return acls.stream().anyMatch(accessControlEntry -> {
                        switch (accessControlEntry.getSpec().getResourcePatternType()) {
                            case PREFIXED:
                                return underlyingTopicName.startsWith(accessControlEntry.getSpec().getResource());
                            case LITERAL:
                                return underlyingTopicName.equals(accessControlEntry.getSpec().getResource());
                        }

                        return false;
                    });
                })
                .map(namespacedSubject -> Schema.builder()
                    .metadata(ObjectMeta.builder()
                            .cluster(namespace.getMetadata().getCluster())
                            .namespace(namespace.getMetadata().getName())
                            .name(namespacedSubject)
                            .build())
                    .build())
                .collect(Collectors.toList());
    }

    /**
     * Get the last version of a schema by namespace and subject
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return A schema
     */
    public Optional<Schema> getLatestSubject(Namespace namespace, String subject) {
        Optional<SchemaResponse> response = kafkaSchemaRegistryClient.
                getLatestSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), subject);

        if(response.isEmpty())
            return Optional.empty();


        Optional<SchemaCompatibilityResponse> compatibilityResponse = kafkaSchemaRegistryClient.
                getCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), subject);

        Schema.Compatibility compatibility = compatibilityResponse.isPresent() ? compatibilityResponse.get().compatibilityLevel() : Schema.Compatibility.GLOBAL;


        return Optional.of(Schema.builder()
                        .metadata(ObjectMeta.builder()
                                .cluster(namespace.getMetadata().getCluster())
                                .namespace(namespace.getMetadata().getName())
                                .name(response.get().subject())
                                .build())
                        .spec(Schema.SchemaSpec.builder()
                                .id(response.get().id())
                                .version(response.get().version())
                                .compatibility(compatibility)
                                .schema(response.get().schema())
                                .schemaType(response.get().schemaType())
                                .build())
                        .build());
    }

    /**
     * Publish a schema
     *
     * @param namespace The namespace
     * @param schema The schema to create
     * @return The ID of the created schema
     */
    public Integer register(Namespace namespace, Schema schema) {
        SchemaResponse response = kafkaSchemaRegistryClient.
                register(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        schema.getMetadata().getName(), SchemaRequest.builder()
                                .schema(schema.getSpec().getSchema())
                                .references(schema.getSpec().getReferences())
                                .build());

        return response.id();
    }

    /**
     * Delete all schemas under the given subject
     *
     * @param namespace The current namespace
     * @param subject The current subject to delete
     */
    public void deleteSubject(Namespace namespace, String subject) {
        kafkaSchemaRegistryClient.
                deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        subject, false);

        kafkaSchemaRegistryClient.
                deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        subject, true);
    }

    /**
     * Validate the schema compatibility
     *
     * @param cluster The cluster
     * @param schema The schema to validate
     * @return A list of errors
     */
    public List<String> validateSchemaCompatibility(String cluster, Schema schema) {
        try {
            Optional<SchemaCompatibilityCheckResponse> response = kafkaSchemaRegistryClient.validateSchemaCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    cluster, schema.getMetadata().getName(), SchemaRequest.builder()
                            .schema(schema.getSpec().getSchema())
                            .references(schema.getSpec().getReferences())
                            .build());

            if (response.isPresent() && !response.get().isCompatible()){
                return response.get().messages();
            }
            // 200 is_compatible OR 404
            return List.of();
        } catch (HttpClientResponseException e) {
            return List.of("An error occurred during the schema validation (status code: " + e.getStatus() + ")");
        }
    }

    /**
     * Update the compatibility of a subject
     *
     * @param namespace The namespace
     * @param schema The schema
     * @param compatibility The compatibility to apply
     * @return A schema compatibility state
     */
    public void updateSubjectCompatibility(Namespace namespace, Schema schema, Schema.Compatibility compatibility) {
        // Reset to default
        if (compatibility.equals(Schema.Compatibility.GLOBAL)) {
            kafkaSchemaRegistryClient.deleteCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    namespace.getMetadata().getCluster(), schema.getMetadata().getName());
        } else {
            // Update
            kafkaSchemaRegistryClient.updateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    namespace.getMetadata().getCluster(), schema.getMetadata().getName(),
                    compatibility.toString());
        }
    }

    /**
     * Does the namespace is owner of the given schema
     *
     * @param namespace The namespace
     * @param subjectName The name of the subject
     * @return true if it's owner, false otherwise
     */
    public boolean isNamespaceOwnerOfSubject(Namespace namespace, String subjectName) {
        String underlyingTopicName = subjectName.replaceAll("(-key|-value)$","");
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(), AccessControlEntry.ResourceType.TOPIC,
                underlyingTopicName);
    }
}
