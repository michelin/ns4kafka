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
     * Latest schema version identifier
     */
    private static final String LATEST_VERSION = "latest";

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
        return this.kafkaSchemaRegistryClient
                .getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())
                .stream()
                //TODO Optimize with accessControlEntryService.findAllGrantedToNamespace
                .filter(subject -> this.isNamespaceOwnerOfSubject(namespace, subject))
                .map(namespacedSubject -> this.getLatestSubject(namespace, namespacedSubject))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Get a schema by subject and version
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


        return Schema.builder()
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
        HttpResponse<SchemaResponse> response = this.kafkaSchemaRegistryClient.
                register(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        schema.getMetadata().getName(), Map.of("schema", schema.getSpec().getSchema()));

        Optional<SchemaResponse> schemaSpecOptional = response.getBody();
        if (schemaSpecOptional.isEmpty()) {
            return Optional.empty();
        }

        return this.getBySubjectAndVersion(namespace, schema.getMetadata().getName(), LATEST_VERSION);
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
        HttpResponse<SchemaCompatibilityCheckResponse> response = this.kafkaSchemaRegistryClient.validateSchemaCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, schema.getMetadata().getName(), Map.of("schema", schema.getSpec().getSchema()));

        // Schema registry return 404 for new subjects
        //TODO I think we need to try/catch HttpClientResponseException instead
        if(response.getStatus() == HttpStatus.NOT_FOUND)
            return List.of();

        // No compatibility issue
        if(response.getStatus() == HttpStatus.OK)
            return List.of();

        if (!response.body().isCompatible()) {
            return response.body().messages();
        }

        return List.of("Unknown error occurred. HttpResponse " + response.code());
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

        return this.getBySubjectAndVersion(namespace, subject, LATEST_VERSION);
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
