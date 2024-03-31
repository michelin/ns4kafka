package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidSchemaReference;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidSchemaResource;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidSchemaSuffix;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SchemaList;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityRequest;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaRequest;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaResponse;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.micronaut.core.util.CollectionUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Service to manage schemas.
 */
@Slf4j
@Singleton
public class SchemaService {
    @Inject
    AccessControlEntryService accessControlEntryService;

    @Inject
    SchemaRegistryClient schemaRegistryClient;

    /**
     * Get all the schemas by namespace.
     *
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
                String underlyingTopicName = subject.replaceAll("(-key|-value)$", "");

                return acls.stream()
                    .anyMatch(accessControlEntry -> switch (accessControlEntry.getSpec().getResourcePatternType()) {
                        case PREFIXED -> underlyingTopicName.startsWith(accessControlEntry.getSpec().getResource());
                        case LITERAL -> underlyingTopicName.equals(accessControlEntry.getSpec().getResource());
                    });
            })
            .map(subject -> SchemaList.builder()
                .metadata(Metadata.builder()
                    .cluster(namespace.getMetadata().getCluster())
                    .namespace(namespace.getMetadata().getName())
                    .name(subject)
                    .build())
                .build());
    }

    /**
     * Get all the subject versions for a given subject.
     *
     * @param namespace The namespace
     * @param subject   The subject
     * @return All the subject versions
     */
    public Flux<Schema> getAllSubjectVersions(Namespace namespace, String subject) {
        return schemaRegistryClient.getAllSubjectVersions(namespace.getMetadata().getCluster(), subject)
            .map(subjectResponse -> Schema.builder()
                .metadata(Metadata.builder()
                    .cluster(namespace.getMetadata().getCluster())
                    .namespace(namespace.getMetadata().getName())
                    .name(subjectResponse.subject())
                    .build())
                .spec(Schema.SchemaSpec.builder()
                    .id(subjectResponse.id())
                    .version(subjectResponse.version())
                    .schema(subjectResponse.schema())
                    .schemaType(subjectResponse.schemaType() == null ? Schema.SchemaType.AVRO :
                        Schema.SchemaType.valueOf(subjectResponse.schemaType()))
                    .references(subjectResponse.references())
                    .build())
                .build()
            );
    }

    /**
     * Get a subject by its name and version.
     *
     * @param namespace The namespace
     * @param subject   The subject
     * @param version   The version
     * @return A Subject
     */
    public Mono<Schema> getSubject(Namespace namespace, String subject, String version) {
        return schemaRegistryClient
            .getSubject(namespace.getMetadata().getCluster(), subject, version)
            .flatMap(latestSubjectOptional -> schemaRegistryClient
                .getCurrentCompatibilityBySubject(namespace.getMetadata().getCluster(), subject)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .map(currentCompatibilityOptional -> {
                    Schema.Compatibility compatibility = currentCompatibilityOptional.isPresent()
                        ? currentCompatibilityOptional.get().compatibilityLevel() : Schema.Compatibility.GLOBAL;

                    return Schema.builder()
                        .metadata(Metadata.builder()
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
     * Get the last version of a schema by namespace and subject.
     *
     * @param namespace The namespace
     * @param subject   The subject
     * @return A schema
     */
    public Mono<Schema> getLatestSubject(Namespace namespace, String subject) {
        return getSubject(namespace, subject, "latest");
    }

    /**
     * Validate a schema when it is created or updated.
     *
     * @param namespace The namespace
     * @param schema    The schema to validate
     * @return A list of errors
     */
    public Mono<List<String>> validateSchema(Namespace namespace, Schema schema) {
        return Mono.defer(() -> {
            List<String> validationErrors = new ArrayList<>();

            // Validate TopicNameStrategy
            // https://github.com/confluentinc/schema-registry/blob/master/schema-serializer/src/main/java/io/confluent/kafka/serializers/subject/TopicNameStrategy.java
            if (!schema.getMetadata().getName().endsWith("-key")
                && !schema.getMetadata().getName().endsWith("-value")) {
                validationErrors.add(invalidSchemaSuffix(schema.getMetadata().getName()));
            }

            if (!CollectionUtils.isEmpty(schema.getSpec().getReferences())) {
                return validateReferences(namespace, schema)
                    .map(referenceErrors -> {
                        validationErrors.addAll(referenceErrors);
                        return validationErrors;
                    });
            }

            return Mono.just(validationErrors);
        });
    }

    /**
     * Validate the references of a schema.
     *
     * @param ns     The namespace
     * @param schema The schema to validate
     * @return A list of errors
     */
    private Mono<List<String>> validateReferences(Namespace ns, Schema schema) {
        return Flux.fromIterable(schema.getSpec().getReferences())
            .flatMap(reference -> getSubject(ns, reference.getSubject(), String.valueOf(reference.getVersion()))
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .mapNotNull(schemaOptional -> {
                    if (schemaOptional.isEmpty()) {
                        return invalidSchemaReference(reference.getSubject(), String.valueOf(reference.getVersion()));
                    }
                    return null;
                }))
            .collectList();
    }

    /**
     * Publish a schema.
     *
     * @param namespace The namespace
     * @param schema    The schema to create
     * @return The ID of the created schema
     */
    public Mono<Integer> register(Namespace namespace, Schema schema) {
        return schemaRegistryClient
            .register(namespace.getMetadata().getCluster(),
                schema.getMetadata().getName(), SchemaRequest.builder()
                    .schemaType(String.valueOf(schema.getSpec().getSchemaType()))
                    .schema(schema.getSpec().getSchema())
                    .references(schema.getSpec().getReferences())
                    .build())
            .map(SchemaResponse::id);
    }

    /**
     * Delete all schemas under the given subject.
     *
     * @param namespace The current namespace
     * @param subject   The current subject to delete
     * @return The list of deleted versions
     */
    public Mono<Integer[]> deleteSubject(Namespace namespace, String subject) {
        return schemaRegistryClient
            .deleteSubject(namespace.getMetadata().getCluster(), subject, false)
            .flatMap(ids -> schemaRegistryClient
                .deleteSubject(namespace.getMetadata().getCluster(),
                    subject, true));
    }

    /**
     * Validate the schema compatibility.
     *
     * @param cluster The cluster
     * @param schema  The schema to validate
     * @return A list of errors
     */
    public Mono<List<String>> validateSchemaCompatibility(String cluster, Schema schema) {
        return schemaRegistryClient.validateSchemaCompatibility(cluster, schema.getMetadata().getName(),
                SchemaRequest.builder()
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
                    return schemaCompatibilityCheckOptional.get().messages()
                        .stream()
                        .map(error -> invalidSchemaResource(schema.getMetadata().getName(), error))
                        .toList();
                }

                return List.of();
            });
    }

    /**
     * Update the compatibility of a subject.
     *
     * @param namespace     The namespace
     * @param schema        The schema
     * @param compatibility The compatibility to apply
     */
    public Mono<SchemaCompatibilityResponse> updateSubjectCompatibility(Namespace namespace, Schema schema,
                                                                        Schema.Compatibility compatibility) {
        if (compatibility.equals(Schema.Compatibility.GLOBAL)) {
            return schemaRegistryClient.deleteCurrentCompatibilityBySubject(namespace.getMetadata().getCluster(),
                schema.getMetadata().getName());
        } else {
            return schemaRegistryClient.updateSubjectCompatibility(namespace.getMetadata().getCluster(),
                schema.getMetadata().getName(), SchemaCompatibilityRequest.builder()
                    .compatibility(compatibility.toString()).build());
        }
    }

    /**
     * Does the namespace is owner of the given schema.
     *
     * @param namespace   The namespace
     * @param subjectName The name of the subject
     * @return true if it's owner, false otherwise
     */
    public boolean isNamespaceOwnerOfSubject(Namespace namespace, String subjectName) {
        String underlyingTopicName = subjectName.replaceAll("(-key|-value)$", "");
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(),
            AccessControlEntry.ResourceType.TOPIC,
            underlyingTopicName);
    }

    /**
     * Get all the schema of all the references for a given schema.
     *
     * @param schema    The schema
     * @param namespace The namespace
     * @return The schema references
     */
    public Mono<Map<String, String>> getSchemaReferences(Schema schema, Namespace namespace) {
        if (CollectionUtils.isEmpty(schema.getSpec().getReferences())) {
            return Mono.just(Collections.emptyMap());
        }

        return Flux.fromIterable(schema.getSpec().getReferences())
            .flatMap(reference -> getSubject(namespace, reference.getSubject(), String.valueOf(reference.getVersion())))
            .collectMap(s -> s.getMetadata().getName(), s -> s.getSpec().getSchema());
    }

    /**
     * Get the schema references.
     *
     * @param schema The schema
     * @return The schema references
     */
    public List<SchemaReference> getReferences(Schema schema) {
        if (CollectionUtils.isEmpty(schema.getSpec().getReferences())) {
            return Collections.emptyList();
        }

        return schema.getSpec().getReferences()
            .stream()
            .map(reference ->
                new SchemaReference(reference.getName(), reference.getSubject(), reference.getVersion()))
            .toList();
    }

    /**
     * Check if the schema already exists in the registry all versions combined.
     *
     * @param namespace  The namespace
     * @param schema     The schema
     * @param oldSchemas The old schemas
     * @return true as Mono if it exists, false otherwise
     */
    public Mono<Boolean> existInOldVersions(Namespace namespace, Schema schema, List<Schema> oldSchemas) {
        return getSchemaReferences(schema, namespace)
            .flatMap(schemaRefs ->
                // If new schema matches any of the existing schemas, return unchanged
                Flux.fromIterable(oldSchemas)
                    .flatMap(oldSchema -> getSchemaReferences(oldSchema, namespace)
                        .map(oldSchemaRefs -> {
                            var currentSchema = new AvroSchema(oldSchema.getSpec().getSchema(),
                                getReferences(oldSchema), oldSchemaRefs, null);

                            var newSchema = new AvroSchema(schema.getSpec().getSchema(),
                                getReferences(schema), schemaRefs, null);

                            return Objects.equals(newSchema.canonicalString(),
                                currentSchema.canonicalString())
                                && Objects.equals(newSchema.references(), currentSchema.references());
                        }))
                    .any(subjectComparison -> subjectComparison)
            );
    }
}
