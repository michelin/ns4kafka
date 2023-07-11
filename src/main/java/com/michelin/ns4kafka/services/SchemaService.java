package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaList;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaCompatibilityRequest;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaRequest;
import com.michelin.ns4kafka.services.clients.schema.entities.SchemaResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class SchemaService {
    @Inject
    AccessControlEntryService accessControlEntryService;

    @Inject
    SchemaRegistryClient schemaRegistryClient;

    /**
     * Get all the schemas by namespace
     * @param namespace The namespace
     * @return A list of schemas
     */
    public Flux<SchemaList> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace).stream()
                .filter(acl -> acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(acl -> acl.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC).toList();

        return schemaRegistryClient
                .getSubjects(namespace.getMetadata().getCluster())
                .filter(subject -> {
                    String underlyingTopicName = subject.replaceAll("(-key|-value)$","");

                    return acls.stream().anyMatch(accessControlEntry -> switch (accessControlEntry.getSpec().getResourcePatternType()) {
                        case PREFIXED ->
                                underlyingTopicName.startsWith(accessControlEntry.getSpec().getResource());
                        case LITERAL ->
                                underlyingTopicName.equals(accessControlEntry.getSpec().getResource());
                    });
                })
                .map(subject -> SchemaList.builder()
                    .metadata(ObjectMeta.builder()
                            .cluster(namespace.getMetadata().getCluster())
                            .namespace(namespace.getMetadata().getName())
                            .name(subject)
                            .build())
                    .build());
    }

    /**
     * Get the last version of a schema by namespace and subject
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return A schema
     */
    public Mono<Schema> getLatestSubject(Namespace namespace, String subject) {
        return schemaRegistryClient
                .getLatestSubject(namespace.getMetadata().getCluster(), subject)
                .flatMap(latestSubjectOptional -> schemaRegistryClient
                    .getCurrentCompatibilityBySubject(namespace.getMetadata().getCluster(), subject)
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty())
                    .map(currentCompatibilityOptional -> {
                        Schema.Compatibility compatibility = currentCompatibilityOptional.isPresent() ? currentCompatibilityOptional.get().compatibilityLevel() : Schema.Compatibility.GLOBAL;

                        return Schema.builder()
                                .metadata(ObjectMeta.builder()
                                        .cluster(namespace.getMetadata().getCluster())
                                        .namespace(namespace.getMetadata().getName())
                                        .name(latestSubjectOptional.subject())
                                        .build())
                                .spec(Schema.SchemaSpec.builder()
                                        .id(latestSubjectOptional.id())
                                        .version(latestSubjectOptional.version())
                                        .compatibility(compatibility)
                                        .schema(latestSubjectOptional.schema())
                                        .schemaType(latestSubjectOptional.schemaType() == null ? Schema.SchemaType.AVRO :
                                                Schema.SchemaType.valueOf(latestSubjectOptional.schemaType()))
                                        .build())
                                .build();
                    }));
    }

    /**
     * Publish a schema
     *
     * @param namespace The namespace
     * @param schema The schema to create
     * @return The ID of the created schema
     */
    public Mono<Integer> register(Namespace namespace, Schema schema) {
        return schemaRegistryClient.
                register(namespace.getMetadata().getCluster(),
                        schema.getMetadata().getName(), SchemaRequest.builder()
                                .schemaType(String.valueOf(schema.getSpec().getSchemaType()))
                                .schema(schema.getSpec().getSchema())
                                .references(schema.getSpec().getReferences())
                                .build())
                .map(SchemaResponse::id);
    }

    /**
     * Delete all schemas under the given subject
     * @param namespace The current namespace
     * @param subject The current subject to delete
     * @return The list of deleted versions
     */
    public Mono<Integer[]> deleteSubject(Namespace namespace, String subject) {
        return schemaRegistryClient
                .deleteSubject(namespace.getMetadata().getCluster(), subject, false)
                .flatMap(ids -> schemaRegistryClient.
                        deleteSubject(namespace.getMetadata().getCluster(),
                                subject, true));
    }

    /**
     * Validate the schema compatibility
     *
     * @param cluster The cluster
     * @param schema The schema to validate
     * @return A list of errors
     */
    public Mono<List<String>> validateSchemaCompatibility(String cluster, Schema schema) {
        return schemaRegistryClient.validateSchemaCompatibility(cluster, schema.getMetadata().getName(), SchemaRequest.builder()
                        .schemaType(String.valueOf(schema.getSpec().getSchemaType()))
                        .schema(schema.getSpec().getSchema())
                        .references(schema.getSpec().getReferences())
                        .build())
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .map(schemaCompatibilityCheckOptional -> {
                    if (schemaCompatibilityCheckOptional.isEmpty()) {
                        return List.of();
                    }

                    if (!schemaCompatibilityCheckOptional.get().isCompatible()) {
                        return schemaCompatibilityCheckOptional.get().messages();
                    }

                    return List.of();
                });
    }

    /**
     * Update the compatibility of a subject
     *
     * @param namespace The namespace
     * @param schema The schema
     * @param compatibility The compatibility to apply
     */
    public Mono<SchemaCompatibilityResponse> updateSubjectCompatibility(Namespace namespace, Schema schema, Schema.Compatibility compatibility) {
        if (compatibility.equals(Schema.Compatibility.GLOBAL)) {
            return schemaRegistryClient.deleteCurrentCompatibilityBySubject(namespace.getMetadata().getCluster(), schema.getMetadata().getName());
        } else {
            return schemaRegistryClient.updateSubjectCompatibility(namespace.getMetadata().getCluster(),
                    schema.getMetadata().getName(), SchemaCompatibilityRequest.builder()
                            .compatibility(compatibility.toString()).build());
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
