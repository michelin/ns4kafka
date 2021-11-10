package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaResponse;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
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
     * Find all schemas on a given namespace
     *
     * @param namespace The namespace used to research the schemas
     * @return A list of schemas
     */
    public List<Schema> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace);

        return this.kafkaSchemaRegistryClient
                .getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())
                .stream()
                .filter(subject -> {
                    String underlyingTopicName = subject.replaceAll("(-key|-value)$","");

                    return acls.stream().anyMatch(accessControlEntry -> {
                        //need to check accessControlEntry.Permission, we want OWNER
                        if (accessControlEntry.getSpec().getPermission() != AccessControlEntry.Permission.OWNER) {
                            return false;
                        }
                        if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC) {
                            switch (accessControlEntry.getSpec().getResourcePatternType()) {
                                case PREFIXED:
                                    return underlyingTopicName.startsWith(accessControlEntry.getSpec().getResource());
                                case LITERAL:
                                    return underlyingTopicName.equals(accessControlEntry.getSpec().getResource());
                            }
                        }
                        return false;
                    });
                })
                .map(namespacedSubject -> this.getLatestSubject(namespace, namespacedSubject))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Get a latest schema by subject
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return The schema
     */
    public Optional<Schema> getLatestSubject(Namespace namespace, String subject) {
        Optional<SchemaResponse> response = this.kafkaSchemaRegistryClient.
                getLatestSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), subject);

        if(response.isEmpty())
            return Optional.empty();


        Optional<SchemaCompatibilityResponse> compatibilityResponse = this.kafkaSchemaRegistryClient.
                getCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), subject);

        Schema.Compatibility compatibility = compatibilityResponse.isPresent() ? compatibilityResponse.get().compatibilityLevel() : Schema.Compatibility.DEFAULT;


        return Optional.of(Schema.builder()
                        .metadata(ObjectMeta.builder()
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
     * Register a schema
     *
     * @param namespace The namespace
     * @param schema The schema to register
     * @return The registered schema
     */
    public Optional<Schema> register(Namespace namespace, Schema schema) {
        Optional<SchemaResponse> response = this.kafkaSchemaRegistryClient.
                register(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        schema.getMetadata().getName(), Map.of("schema", schema.getSpec().getSchema()));

        if(response.isEmpty())
            return Optional.empty();

        return this.getLatestSubject(namespace, schema.getMetadata().getName());
    }

    /**
     * Delete a subject
     *
     * @param namespace The namespace
     * @param subject The subject to delete
     */
    public void deleteSubject(Namespace namespace, String subject) {
        this.kafkaSchemaRegistryClient.
                deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        subject, false);

        this.kafkaSchemaRegistryClient.
                deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        subject, true);
    }

    /**
     * Validate the schema compatibility against the Schema Registry
     *
     * @param cluster The cluster
     * @param schema The schema to validate
     * @return A list of errors
     */
    public List<String> validateSchemaCompatibility(String cluster, Schema schema) {
        SchemaCompatibilityCheckResponse response = this.kafkaSchemaRegistryClient.validateSchemaCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, schema.getMetadata().getName(), Map.of("schema", schema.getSpec().getSchema()));

        if (!response.isCompatible()) {
            return response.messages();
        }

        return List.of();
    }
    /**
     * Update the schema compatibility against the Schema Registry
     *
     * @param namespace The namespace
     * @param subject The subject
     * @param compatibility The compatibility
     */
    public Optional<Schema> updateSubjectCompatibility(Namespace namespace, String subject, Schema.Compatibility compatibility) {
        if (compatibility.equals(Schema.Compatibility.DEFAULT)) {
            this.kafkaSchemaRegistryClient.deleteCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    namespace.getMetadata().getCluster(), subject);
        } else {
            this.kafkaSchemaRegistryClient.updateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    namespace.getMetadata().getCluster(), subject,
                    Map.of("compatibility", compatibility.toString()));
        }

        return this.getLatestSubject(namespace, subject);
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
        return this.accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(), AccessControlEntry.ResourceType.TOPIC,
                underlyingTopicName);
    }
}
