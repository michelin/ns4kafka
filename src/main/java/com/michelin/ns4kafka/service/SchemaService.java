/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TOPIC;
import static com.michelin.ns4kafka.model.schema.Schema.SchemaType.AVRO;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidSchemaReference;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidSchemaResource;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidSchemaSubjectName;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SubjectNameStrategy;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityRequest;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaCompatibilityResponse;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaRequest;
import com.michelin.ns4kafka.service.client.schema.entities.SchemaResponse;
import com.michelin.ns4kafka.util.RegexUtils;
import com.michelin.ns4kafka.validation.AuthorizedSubjectNameStrategy;
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
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Service to manage schemas. */
@Slf4j
@Singleton
public class SchemaService {
    private static final String KEY_SCHEMA_SUFFIX = "-key";
    private static final String VALUE_SCHEMA_SUFFIX = "-value";

    @Inject
    private AclService aclService;

    @Inject
    private SchemaRegistryClient schemaRegistryClient;

    /**
     * Get all the schemas of a given namespace.
     *
     * @param namespace The namespace
     * @return A list of schemas
     */
    public Flux<Schema> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = aclService.findResourceOwnerGrantedToNamespace(namespace, TOPIC);
        return schemaRegistryClient
                .getSubjects(namespace.getMetadata().getCluster())
                .filter(subject -> aclService.isResourceCoveredByAcls(acls, extractResourceNameFromSubject(subject)))
                .map(subject -> Schema.builder()
                        .metadata(Metadata.builder()
                                .cluster(namespace.getMetadata().getCluster())
                                .namespace(namespace.getMetadata().getName())
                                .name(subject)
                                .build())
                        .build());
    }

    /**
     * Get all the schemas of a given namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name filter
     * @return A list of schemas
     */
    public Flux<Schema> findByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAllForNamespace(namespace)
                .filter(schema ->
                        RegexUtils.isResourceCoveredByRegex(schema.getMetadata().getName(), nameFilterPatterns));
    }

    /**
     * Get all the subject versions for a given subject.
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return All the subject versions
     */
    public Flux<Schema> getAllSubjectVersions(Namespace namespace, String subject) {
        return schemaRegistryClient
                .getAllSubjectVersions(namespace.getMetadata().getCluster(), subject)
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
                                .schemaType(
                                        subjectResponse.schemaType() == null
                                                ? AVRO
                                                : Schema.SchemaType.valueOf(subjectResponse.schemaType()))
                                .references(subjectResponse.references())
                                .build())
                        .build());
    }

    /**
     * Build the schema spec from the SchemaResponse.
     *
     * @param namespace The namespace
     * @param subjectOptional The subject object from Http response
     * @return A Subject
     */
    public Mono<Schema> buildSchemaSpec(Namespace namespace, SchemaResponse subjectOptional) {
        return schemaRegistryClient
                .getCurrentCompatibilityBySubject(namespace.getMetadata().getCluster(), subjectOptional.subject())
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .map(currentCompatibilityOptional -> {
                    Schema.Compatibility compatibility = currentCompatibilityOptional.isPresent()
                            ? currentCompatibilityOptional.get().compatibilityLevel()
                            : Schema.Compatibility.GLOBAL;

                    return Schema.builder()
                            .metadata(Metadata.builder()
                                    .cluster(namespace.getMetadata().getCluster())
                                    .namespace(namespace.getMetadata().getName())
                                    .name(subjectOptional.subject())
                                    .build())
                            .spec(Schema.SchemaSpec.builder()
                                    .id(subjectOptional.id())
                                    .version(subjectOptional.version())
                                    .compatibility(compatibility)
                                    .schema(subjectOptional.schema())
                                    .schemaType(
                                            subjectOptional.schemaType() == null
                                                    ? AVRO
                                                    : Schema.SchemaType.valueOf(subjectOptional.schemaType()))
                                    .references(subjectOptional.references())
                                    .build())
                            .build();
                });
    }

    /**
     * Get a subject by its name and version.
     *
     * @param namespace The namespace
     * @param subject The subject
     * @param version The version
     * @return A subject
     */
    public Mono<Schema> getSubjectByVersion(Namespace namespace, String subject, String version) {
        return schemaRegistryClient
                .getSubject(namespace.getMetadata().getCluster(), subject, version)
                .flatMap(subjectOptional -> buildSchemaSpec(namespace, subjectOptional));
    }

    /**
     * Get the last version of a schema by namespace and subject.
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return A schema
     */
    public Mono<Schema> getSubjectLatestVersion(Namespace namespace, String subject) {
        return getSubjectByVersion(namespace, subject, "latest");
    }

    /**
     * Validate a schema when it is created or updated.
     *
     * @param namespace The namespace
     * @param schema The schema to validate
     * @return A list of errors
     */
    public Mono<List<String>> validateSchema(Namespace namespace, Schema schema) {
        return Mono.defer(() -> {
            AuthorizedSubjectNameStrategy authorizedStrategies = getAuthorizedSubjectNameStrategies(namespace);
            return validateSubjectStrategy(namespace, schema, authorizedStrategies)
                    .flatMap(errors -> {
                        if (CollectionUtils.isEmpty(schema.getSpec().getReferences())) {
                            return Mono.just(errors);
                        }

                        return validateReferences(namespace, schema).map(referenceErrors -> {
                            errors.addAll(referenceErrors);
                            return errors;
                        });
                    });
        });
    }

    /**
     * Validate the references of a schema.
     *
     * @param ns The namespace
     * @param schema The schema to validate
     * @return A list of errors
     */
    private Mono<List<String>> validateReferences(Namespace ns, Schema schema) {
        return Flux.fromIterable(schema.getSpec().getReferences())
                .flatMap(reference -> getSubjectByVersion(
                                ns, reference.getSubject(), String.valueOf(reference.getVersion()))
                        .map(Optional::of)
                        .defaultIfEmpty(Optional.empty())
                        .mapNotNull(schemaOptional -> {
                            if (schemaOptional.isEmpty()) {
                                return invalidSchemaReference(
                                        reference.getSubject(), String.valueOf(reference.getVersion()));
                            }
                            return null;
                        }))
                .collectList();
    }

    /**
     * Publish a schema.
     *
     * @param namespace The namespace
     * @param schema The schema to create
     * @return The ID of the created schema
     */
    public Mono<Integer> register(Namespace namespace, Schema schema) {
        return schemaRegistryClient
                .register(
                        namespace.getMetadata().getCluster(),
                        schema.getMetadata().getName(),
                        SchemaRequest.builder()
                                .schemaType(String.valueOf(schema.getSpec().getSchemaType()))
                                .schema(schema.getSpec().getSchema())
                                .references(schema.getSpec().getReferences())
                                .build())
                .map(SchemaResponse::id);
    }

    /**
     * Delete all the schema versions under the given subject.
     *
     * @param namespace The namespace
     * @param subject The subject to delete
     * @return The list of deleted schema versions
     */
    public Mono<Integer[]> deleteAllVersions(Namespace namespace, String subject) {
        return schemaRegistryClient
                .deleteSubject(namespace.getMetadata().getCluster(), subject, false)
                .flatMap(_ -> schemaRegistryClient.deleteSubject(
                        namespace.getMetadata().getCluster(), subject, true));
    }

    /**
     * Delete the schema version under the given subject.
     *
     * @param namespace The namespace
     * @param subject The subject
     * @param version The version of the schema to delete
     * @return The deleted schema version
     */
    public Mono<Integer> deleteVersion(Namespace namespace, String subject, String version) {
        return schemaRegistryClient
                .deleteSubjectVersion(namespace.getMetadata().getCluster(), subject, version, false)
                .flatMap(softDeletedVersionIds -> schemaRegistryClient.deleteSubjectVersion(
                        namespace.getMetadata().getCluster(), subject, Integer.toString(softDeletedVersionIds), true));
    }

    /**
     * Validate the schema compatibility.
     *
     * @param cluster The cluster
     * @param schema The schema to validate
     * @return A list of errors
     */
    public Mono<List<String>> validateSchemaCompatibility(String cluster, Schema schema) {
        return schemaRegistryClient
                .validateSchemaCompatibility(
                        cluster,
                        schema.getMetadata().getName(),
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
                        return schemaCompatibilityCheckOptional.get().messages().stream()
                                .map(error -> invalidSchemaResource(
                                        schema.getMetadata().getName(), error))
                                .toList();
                    }

                    return List.of();
                });
    }

    /**
     * Update the compatibility of a subject.
     *
     * @param namespace The namespace
     * @param schema The schema
     * @param compatibility The compatibility to apply
     */
    public Mono<SchemaCompatibilityResponse> updateSubjectCompatibility(
            Namespace namespace, Schema schema, Schema.Compatibility compatibility) {
        if (compatibility.equals(Schema.Compatibility.GLOBAL)) {
            return schemaRegistryClient.deleteCurrentCompatibilityBySubject(
                    namespace.getMetadata().getCluster(), schema.getMetadata().getName());
        } else {
            return schemaRegistryClient.updateSubjectCompatibility(
                    namespace.getMetadata().getCluster(),
                    schema.getMetadata().getName(),
                    SchemaCompatibilityRequest.builder()
                            .compatibility(compatibility.toString())
                            .build());
        }
    }

    /**
     * Does the namespace is owner of the given schema.
     *
     * <p>For topic name strategy and topic record name strategy, the validation is done against the topic name.
     *
     * <p>For record name strategy, the validation is done against the record name. This requires having a prefixed ACL
     * with a dot (e.g., "abc.") to cover the record name "abc.package.name.MyRecord". There's probably a better way to
     * handle this.
     *
     * @param namespace The namespace
     * @param subjectName The name of the subject
     * @return true if it's owner, false otherwise
     */
    public boolean isNamespaceOwnerOfSubject(Namespace namespace, String subjectName) {
        return aclService.isNamespaceOwnerOfResource(
                namespace.getMetadata().getName(), TOPIC, extractResourceNameFromSubject(subjectName));
    }

    /**
     * Get all the schema of all the references for a given schema.
     *
     * @param namespace The namespace
     * @param schema The schema
     * @return The schema references
     */
    public Mono<Map<String, String>> getSchemaReferences(Namespace namespace, Schema schema) {
        if (CollectionUtils.isEmpty(schema.getSpec().getReferences())) {
            return Mono.just(Collections.emptyMap());
        }

        return Flux.fromIterable(schema.getSpec().getReferences())
                .flatMap(reference ->
                        getSubjectByVersion(namespace, reference.getSubject(), String.valueOf(reference.getVersion())))
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

        return schema.getSpec().getReferences().stream()
                .map(reference ->
                        new SchemaReference(reference.getName(), reference.getSubject(), reference.getVersion()))
                .toList();
    }

    /**
     * Check if the schema already exists in the registry all versions combined.
     *
     * @param namespace The namespace
     * @param schema The schema
     * @param oldSchemas The old schemas
     * @return true as Mono if it exists, false otherwise
     */
    public Mono<Boolean> existInOldVersions(Namespace namespace, Schema schema, List<Schema> oldSchemas) {
        return getSchemaReferences(namespace, schema)
                .flatMap(schemaRefs ->
                        // If new schema matches any of the existing schemas, return unchanged
                        Flux.fromIterable(oldSchemas)
                                .flatMap(oldSchema -> getSchemaReferences(namespace, oldSchema)
                                        .map(oldSchemaRefs -> {
                                            var currentSchema = new AvroSchema(
                                                    oldSchema.getSpec().getSchema(),
                                                    getReferences(oldSchema),
                                                    oldSchemaRefs,
                                                    null);

                                            var newSchema = new AvroSchema(
                                                    schema.getSpec().getSchema(),
                                                    getReferences(schema),
                                                    schemaRefs,
                                                    null);

                                            return Objects.equals(
                                                            newSchema.canonicalString(),
                                                            currentSchema.canonicalString())
                                                    && Objects.equals(
                                                            newSchema.references(), currentSchema.references());
                                        }))
                                .any(subjectComparison -> subjectComparison));
    }

    /**
     * Get the authorized subject name strategies for a namespace.
     *
     * @param namespace The namespace
     * @return A list of valid subject name strategies
     */
    private AuthorizedSubjectNameStrategy getAuthorizedSubjectNameStrategies(Namespace namespace) {
        if (namespace.getSpec().getTopicValidator() != null
                && CollectionUtils.isNotEmpty(
                        namespace.getSpec().getTopicValidator().getValidationConstraints())) {
            return namespace.getSpec().getTopicValidator().getAuthorizedSubjectNameStrategies();
        }

        return AuthorizedSubjectNameStrategy.defaultStrategies();
    }

    /**
     * Validate the subject name of a schema according to the allowed strategies. If the schema is a key schema (i.e.
     * subject name ends with -key), only the key strategies are considered. If the schema is a value schema (i.e.
     * subject name ends with -value), only the value strategies are considered. Otherwise, all strategies are
     * considered.
     *
     * @param namespace The namespace
     * @param schema The schema
     * @param authorizedStrategies The valid subject name strategies
     * @return A list of errors
     */
    private Mono<List<String>> validateSubjectStrategy(
            Namespace namespace, Schema schema, AuthorizedSubjectNameStrategy authorizedStrategies) {
        List<String> errors = new ArrayList<>();

        if (schema.getMetadata().getName().endsWith(KEY_SCHEMA_SUFFIX)) {
            return Flux.fromIterable(authorizedStrategies.getKeyStrategies())
                    .flatMap(strategy -> matchesStrategy(namespace, strategy, schema))
                    .any(matches -> matches)
                    .map(valid -> {
                        if (Boolean.FALSE.equals(valid)) {
                            errors.add(invalidSchemaSubjectName(
                                    schema.getMetadata().getName(), authorizedStrategies.getKeyStrategies()));
                        }

                        return errors;
                    });
        }

        if (schema.getMetadata().getName().endsWith(VALUE_SCHEMA_SUFFIX)) {
            return Flux.fromIterable(authorizedStrategies.getValueStrategies())
                    .flatMap(strategy -> matchesStrategy(namespace, strategy, schema))
                    .any(matches -> matches)
                    .map(valid -> {
                        if (Boolean.FALSE.equals(valid)) {
                            errors.add(invalidSchemaSubjectName(
                                    schema.getMetadata().getName(), authorizedStrategies.getValueStrategies()));
                        }

                        return errors;
                    });
        }

        return Flux.fromIterable(authorizedStrategies.all())
                .flatMap(strategy -> matchesStrategy(namespace, strategy, schema))
                .any(matches -> matches)
                .map(valid -> {
                    if (Boolean.FALSE.equals(valid)) {
                        errors.add(invalidSchemaSubjectName(
                                schema.getMetadata().getName(), authorizedStrategies.all()));
                    }

                    return errors;
                });
    }

    /**
     * Validates that a subject matches the given naming strategy.
     *
     * @param namespace The namespace
     * @param strategy The naming strategy for a schema subject name
     * @param schema The schema to validate
     * @return Mono of true if the subject name is valid for the given strategy, false otherwise
     * @see <a href=
     *     "https://github.com/confluentinc/schema-registry/blob/master/schema-serializer/src/main/java/io/confluent/kafka/serializers/subject">Schema
     *     Registry strategies</a>
     */
    public Mono<Boolean> matchesStrategy(Namespace namespace, SubjectNameStrategy strategy, Schema schema) {
        String subjectName = schema.getMetadata().getName();
        if (subjectName == null || subjectName.trim().isEmpty()) {
            return Mono.just(false);
        }

        return switch (strategy) {
            case TOPIC_NAME ->
                Mono.just(subjectName.endsWith(KEY_SCHEMA_SUFFIX) || subjectName.endsWith(VALUE_SCHEMA_SUFFIX));
            case RECORD_NAME ->
                extractRecordName(namespace, schema).map(subjectName::equals).defaultIfEmpty(false);
            case TOPIC_RECORD_NAME ->
                extractRecordName(namespace, schema)
                        .map(recordName -> subjectName.endsWith("-" + recordName))
                        .defaultIfEmpty(false);
        };
    }

    /**
     * Extracts the record name from schema.
     *
     * @param namespace The namespace
     * @param schema The schema
     * @return The record name if it can be determined
     */
    public Mono<String> extractRecordName(Namespace namespace, Schema schema) {
        if (StringUtils.isBlank(schema.getSpec().getSchema())) {
            return Mono.empty();
        }

        if (AVRO.equals(schema.getSpec().getSchemaType())) {
            return getSchemaReferences(namespace, schema).map(schemaRefs -> new AvroSchema(
                            schema.getSpec().getSchema(), getReferences(schema), schemaRefs, null)
                    .name());
        }

        return Mono.empty();
    }

    /**
     * From a subject that could be named with any strategy, extract the resource name to validate ownership. For topic
     * name strategy and topic record name strategy, the resource name is the topic name. For record name strategy, the
     * resource name is the record name.
     *
     * @param subject The subject
     * @return The topic name if it can be determined by any of the strategies
     */
    public String extractResourceNameFromSubject(String subject) {
        // Topic name strategy
        if (subject.endsWith(KEY_SCHEMA_SUFFIX) || subject.endsWith(VALUE_SCHEMA_SUFFIX)) {
            return subject.replaceAll("(-key|-value)$", "");
        }

        // Record name strategy
        if (!subject.contains("-")) {
            return subject;
        }

        // Topic record name strategy
        return subject.substring(0, subject.lastIndexOf("-"));
    }
}
