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
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
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
    public List<Schema> getAllByNamespace(Namespace namespace) {
        HttpResponse<List<String>> subjectsResponse = this.kafkaSchemaRegistryClient.
                getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster());

        Optional<List<String>> subjectsOptional = subjectsResponse.getBody();
        if (subjectsOptional.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> namespacedSubjects = subjectsOptional.get()
                .stream()
                .filter(subject -> this.isNamespaceOwnerOfSubject(namespace, subject))
                .collect(Collectors.toList());

        return namespacedSubjects
                .stream()
                .map(namespacedSubject -> this.getBySubjectAndVersion(namespace, namespacedSubject, LATEST_VERSION))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Get a schema by subject and version
     *
     * @param namespace The namespace
     * @param subject The subject
     * @param version The version
     * @return The schema
     */
    public Optional<Schema> getBySubjectAndVersion(Namespace namespace, String subject, String version) {
        HttpResponse<SchemaResponse> response = this.kafkaSchemaRegistryClient.
                getSchemaBySubjectAndVersion(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                        subject, version);

        Schema.SchemaSpec.SchemaSpecBuilder schemaSpecBuilder = Schema.SchemaSpec.builder();

        Optional<SchemaResponse> responseBody = response.getBody();
        if (responseBody.isPresent()) {
            HttpResponse<SchemaCompatibilityResponse> compatibilityResponse = this.kafkaSchemaRegistryClient.
                    getCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                            subject);

            Optional<SchemaCompatibilityResponse> compatibilityResponseBody = compatibilityResponse.getBody();
            compatibilityResponseBody.ifPresent(schemaCompatibilityResponse -> schemaSpecBuilder
                            .compatibility(schemaCompatibilityResponse.compatibilityLevel())
                            .build());
        }

        return responseBody
                .map(schemaResponse -> Schema.builder()
                        .metadata(ObjectMeta.builder()
                                .name(schemaResponse.subject())
                                .build())
                        .spec(schemaSpecBuilder
                                .id(schemaResponse.id())
                                .version(schemaResponse.version())
                                .schema(schemaResponse.schema())
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
                        schema.getMetadata().getName(), Collections.singletonMap("schema", schema.getSpec().getSchema()));

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
                cluster, schema.getMetadata().getName(), Collections.singletonMap("schema", schema.getSpec().getSchema()));

        Optional<SchemaCompatibilityCheckResponse> compatibilityOptional = response.getBody();
        if (compatibilityOptional.isPresent() && !compatibilityOptional.get().isCompatible()) {
            return compatibilityOptional.get().messages();
        }

        return Collections.emptyList();
    }

    /**
     * Update the schema compatibility against the Schema Registry
     *
     * @param namespace The namespace
     * @param subject The subject
     * @param compatibility The compatibility
     */
    public Optional<Schema> updateSubjectCompatibility(Namespace namespace, String subject, Schema.Compatibility compatibility) {
        if (compatibility.equals(Schema.Compatibility.GLOBAL)) {
            this.kafkaSchemaRegistryClient.deleteCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    namespace.getMetadata().getCluster(), subject);
        } else {
            this.kafkaSchemaRegistryClient.updateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    namespace.getMetadata().getCluster(), subject,
                    Collections.singletonMap("compatibility", compatibility.toString()));
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
        return this.accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(), AccessControlEntry.ResourceType.SCHEMA,
                subjectName);
    }
}
