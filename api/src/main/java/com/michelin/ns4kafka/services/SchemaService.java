package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaList;
import com.michelin.ns4kafka.services.schema.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaRequest;
import com.michelin.ns4kafka.services.schema.client.entities.SchemaResponse;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.reactivex.Maybe;
import io.reactivex.Single;
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
     * @param namespace The namespace
     * @return A list of schemas
     */
    public Single<List<SchemaList>> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace).stream()
                .filter(acl -> acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(acl -> acl.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC)
                .collect(Collectors.toList());

        return kafkaSchemaRegistryClient
                .getSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster())
                .map(subjects -> subjects
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
                        .map(namespacedSubject -> SchemaList.builder()
                                .metadata(ObjectMeta.builder()
                                        .cluster(namespace.getMetadata().getCluster())
                                        .namespace(namespace.getMetadata().getName())
                                        .name(namespacedSubject)
                                        .build())
                                .build())
                        .collect(Collectors.toList())
                );
    }

    /**
     * Get the last version of a schema by namespace and subject
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return A schema
     */
    public Maybe<Schema> getLatestSubject(Namespace namespace, String subject) {
        return kafkaSchemaRegistryClient
                .getLatestSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), subject)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMap(latestSubjectOptional -> {
                    if (latestSubjectOptional.isEmpty()) {
                        return Maybe.empty();
                    }

                    return kafkaSchemaRegistryClient
                            .getCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), subject)
                            .map(Optional::of)
                            .defaultIfEmpty(Optional.empty())
                            .map(currentCompatibilityOptional -> {
                                Schema.Compatibility compatibility = currentCompatibilityOptional.isPresent() ? currentCompatibilityOptional.get().compatibilityLevel() : Schema.Compatibility.GLOBAL;

                                return Schema.builder()
                                        .metadata(ObjectMeta.builder()
                                                .cluster(namespace.getMetadata().getCluster())
                                                .namespace(namespace.getMetadata().getName())
                                                .name(latestSubjectOptional.get().subject())
                                                .build())
                                        .spec(Schema.SchemaSpec.builder()
                                                .id(latestSubjectOptional.get().id())
                                                .version(latestSubjectOptional.get().version())
                                                .compatibility(compatibility)
                                                .schema(latestSubjectOptional.get().schema())
                                                .schemaType(latestSubjectOptional.get().schemaType() == null ? Schema.SchemaType.AVRO :
                                                        Schema.SchemaType.valueOf(latestSubjectOptional.get().schemaType()))
                                                .build())
                                        .build();
                            });
                });
    }

    /**
     * Publish a schema
     *
     * @param namespace The namespace
     * @param schema The schema to create
     * @return The ID of the created schema
     */
    public Single<Integer> register(Namespace namespace, Schema schema) {
        return kafkaSchemaRegistryClient.
                register(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
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
    public Single<Integer[]> deleteSubject(Namespace namespace, String subject) {
        return kafkaSchemaRegistryClient
                .deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(), subject, false)
                .flatMap(ids -> kafkaSchemaRegistryClient.
                        deleteSubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, namespace.getMetadata().getCluster(),
                                subject, true));
    }

    /**
     * Validate the schema compatibility
     *
     * @param cluster The cluster
     * @param schema The schema to validate
     * @return A list of errors
     */
    public Single<List<String>> validateSchemaCompatibility(String cluster, Schema schema) {
        return kafkaSchemaRegistryClient.validateSchemaCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, schema.getMetadata().getName(), SchemaRequest.builder()
                        .schemaType(String.valueOf(schema.getSpec().getSchemaType()))
                        .schema(schema.getSpec().getSchema())
                        .references(schema.getSpec().getReferences())
                        .build())
                .flatMap(schemaCompatibilityCheckSuccess -> {
                            if (!schemaCompatibilityCheckSuccess.isCompatible()) {
                                return Maybe.just(schemaCompatibilityCheckSuccess.messages());
                            } else {
                                return Maybe.just(List.<String>of());
                            }
                        },
                        schemaCompatibilityCheckError -> Maybe.just(List.of("An error occurred during the schema validation (status code: " + ((HttpClientResponseException) schemaCompatibilityCheckError).getStatus() + ")")),
                        () -> Maybe.just(List.<String>of()))
                .flatMapSingle(Single::just);
    }

    /**
     * Update the compatibility of a subject
     *
     * @param namespace The namespace
     * @param schema The schema
     * @param compatibility The compatibility to apply
     */
    public Single<SchemaCompatibilityResponse> updateSubjectCompatibility(Namespace namespace, Schema schema, Schema.Compatibility compatibility) {
        // Reset to default
        if (compatibility.equals(Schema.Compatibility.GLOBAL)) {
            return kafkaSchemaRegistryClient.deleteCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    namespace.getMetadata().getCluster(), schema.getMetadata().getName());
        } else {
            // Update
            return kafkaSchemaRegistryClient.updateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
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
