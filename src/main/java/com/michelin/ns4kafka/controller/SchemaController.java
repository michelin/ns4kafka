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

package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.util.enumation.Kind.SCHEMA;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SchemaCompatibilityState;
import com.michelin.ns4kafka.service.SchemaService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Controller to manage schemas.
 */
@Tag(name = "Schemas", description = "Manage the schemas.")
@Controller(value = "/api/namespaces/{namespace}/schemas")
@ExecuteOn(TaskExecutors.IO)
public class SchemaController extends NamespacedResourceController {
    @Inject
    SchemaService schemaService;

    /**
     * List schemas by namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name      The name parameter
     * @return A list of schemas
     */
    @Get
    public Flux<Schema> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        Namespace ns = getNamespace(namespace);
        return schemaService.findByWildcardName(ns, name)
            .collectList()
            .flatMapMany(schemas -> schemas.size() == 1
                ? Flux.fromIterable(schemas
                .stream()
                .map(schema -> schemaService.getSubjectLatestVersion(ns, schema.getMetadata().getName()))
                .toList()).flatMap(schema -> schema)
                : Flux.fromIterable(schemas));
    }

    /**
     * Get the last version of a schema by namespace and subject.
     *
     * @param namespace The namespace
     * @param subject   The subject
     * @return A schema
     * @deprecated use {@link #list(String, String)} instead.
     */
    @Get("/{subject}")
    @Deprecated(since = "1.12.0")
    public Mono<Schema> get(String namespace, String subject) {
        Namespace ns = getNamespace(namespace);

        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Mono.empty();
        }

        return schemaService.getSubjectLatestVersion(ns, subject);
    }

    /**
     * Publish a schema.
     *
     * @param namespace The namespace
     * @param schema    The schema to create
     * @param dryrun    Is dry run mode or not?
     * @return The created schema
     */
    @Post
    public Mono<HttpResponse<Schema>> apply(String namespace,
                                            @Valid @Body Schema schema,
                                            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        if (!schemaService.isNamespaceOwnerOfSubject(ns, schema.getMetadata().getName())) {
            return Mono.error(new ResourceValidationException(schema,
                invalidOwner(schema.getMetadata().getName())));
        }

        return schemaService.validateSchema(ns, schema)
            .flatMap(errors -> {
                if (!errors.isEmpty()) {
                    return Mono.error(
                        new ResourceValidationException(schema, errors));
                }

                return schemaService.getAllSubjectVersions(ns, schema.getMetadata().getName())
                    .collectList()
                    .flatMap(oldSchemas -> schemaService.existInOldVersions(ns, schema, oldSchemas)
                        .flatMap(exist -> {
                            if (Boolean.TRUE.equals(exist)) {
                                return Mono.just(formatHttpResponse(schema, ApplyStatus.unchanged));
                            }

                            return schemaService
                                .validateSchemaCompatibility(ns.getMetadata().getCluster(), schema)
                                .flatMap(validationErrors -> {
                                    if (!validationErrors.isEmpty()) {
                                        return Mono.error(new ResourceValidationException(schema, validationErrors));
                                    }

                                    schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                                    schema.getMetadata().setCluster(ns.getMetadata().getCluster());
                                    schema.getMetadata().setNamespace(ns.getMetadata().getName());

                                    ApplyStatus status =
                                        oldSchemas.isEmpty() ? ApplyStatus.created : ApplyStatus.changed;
                                    if (dryrun) {
                                        return Mono.just(formatHttpResponse(schema, status));
                                    }

                                    return schemaService
                                        .register(ns, schema)
                                        .map(id -> {
                                            sendEventLog(
                                                schema,
                                                status,
                                                oldSchemas.isEmpty() ? null : oldSchemas.stream()
                                                    .max(Comparator.comparingInt((Schema s) -> s.getSpec().getId())),
                                                schema.getSpec(),
                                                EMPTY_STRING
                                            );

                                            return formatHttpResponse(schema, status);
                                        });
                                });
                        }));
            });
    }

    /**
     * Delete all schema versions or a specific schema version if specified, under all given subjects.
     *
     * @param namespace       The namespace
     * @param name            The subject name parameter
     * @param versionOptional The version of the schemas to delete
     * @param dryrun          Run in dry mode or not?
     * @return A HTTP response
     */
    @Delete
    @Status(HttpStatus.OK)
    public Mono<HttpResponse<List<Schema>>> bulkDelete(String namespace,
                                                       @QueryValue(defaultValue = "*") String name,
                                                       @QueryValue("version") Optional<String> versionOptional,
                                                       @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        return schemaService.findByWildcardName(ns, name)
            .flatMap(schema -> versionOptional
                .map(version -> schemaService.getSubjectByVersion(ns, schema.getMetadata().getName(), version))
                .orElseGet(() -> schemaService.getSubjectLatestVersion(ns, schema.getMetadata().getName()))
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty()))
            .collectList()
            .flatMap(optionalSchemas -> {
                if (optionalSchemas.isEmpty() || optionalSchemas.stream().anyMatch(Optional::isEmpty)) {
                    return Mono.just(HttpResponse.notFound());
                }

                List<Schema> schemas = optionalSchemas
                    .stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .toList();

                if (dryrun) {
                    return Mono.just(HttpResponse.ok(schemas));
                }

                return Flux.fromIterable(schemas)
                    .flatMap(schema -> (versionOptional.isEmpty()
                        ? schemaService.deleteAllVersions(ns, schema.getMetadata().getName()) :
                        schemaService.deleteVersion(ns, schema.getMetadata().getName(), versionOptional.get()))
                        .flatMap(deletedVersionIds -> {
                            sendEventLog(
                                schema,
                                ApplyStatus.deleted,
                                schema.getSpec(),
                                null,
                                versionOptional.map(v -> String.valueOf(deletedVersionIds))
                                    .orElse(EMPTY_STRING)
                            );
                            return Mono.just(HttpResponse.noContent());
                        }))
                    .then(Mono.just(HttpResponse.ok(schemas)));
            });
    }

    /**
     * Delete all schema versions or a specific schema version if specified, under the given subject.
     *
     * @param namespace       The namespace
     * @param subject         The subject
     * @param versionOptional The version of the schema to delete
     * @param dryrun          Run in dry mode or not?
     * @return A HTTP response
     * @deprecated use {@link #bulkDelete(String, String, Optional, boolean)} instead.
     */
    @Delete("/{subject}")
    @Deprecated(since = "1.13.0")
    @Status(HttpStatus.NO_CONTENT)
    public Mono<HttpResponse<Void>> delete(String namespace,
                                           @PathVariable String subject,
                                           @QueryValue("version") Optional<String> versionOptional,
                                           @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate ownership
        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Mono.error(new ResourceValidationException(SCHEMA, subject, invalidOwner(subject)));
        }

        return versionOptional
            // If version is specified, get the schema with the version
            .map(version -> schemaService.getSubjectByVersion(ns, subject, version))
            // If version is not specified, get the latest schema
            .orElseGet(() -> schemaService.getSubjectLatestVersion(ns, subject))
            .map(Optional::of)
            .defaultIfEmpty(Optional.empty())
            .flatMap(subjectOptional -> {
                if (subjectOptional.isEmpty()) {
                    return Mono.just(HttpResponse.notFound());
                }

                if (dryrun) {
                    return Mono.just(HttpResponse.noContent());
                }

                return (versionOptional.isEmpty()
                    ? schemaService.deleteAllVersions(ns, subject) :
                    schemaService.deleteVersion(ns, subject, versionOptional.get()))
                    .map(deletedVersionIds -> {
                        Schema deletedSchema = subjectOptional.get();

                        sendEventLog(
                            deletedSchema,
                            ApplyStatus.deleted,
                            deletedSchema.getSpec(),
                            null,
                            versionOptional.map(v -> String.valueOf(deletedVersionIds)).orElse(EMPTY_STRING)
                        );

                        return HttpResponse.noContent();
                    });
            });
    }

    /**
     * Update the compatibility of a subject.
     *
     * @param namespace     The namespace
     * @param subject       The subject to update
     * @param compatibility The compatibility to apply
     * @return A schema compatibility state
     */
    @Post("/{subject}/config")
    public Mono<HttpResponse<SchemaCompatibilityState>> config(String namespace,
                                                               @PathVariable String subject,
                                                               Schema.Compatibility compatibility) {
        Namespace ns = getNamespace(namespace);

        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Mono.error(new ResourceValidationException(SCHEMA, subject, invalidOwner(subject)));
        }

        return schemaService.getSubjectLatestVersion(ns, subject)
            .map(Optional::of)
            .defaultIfEmpty(Optional.empty())
            .flatMap(latestSubjectOptional -> {
                if (latestSubjectOptional.isEmpty()) {
                    return Mono.just(HttpResponse.notFound());
                }

                SchemaCompatibilityState state = SchemaCompatibilityState.builder()
                    .metadata(latestSubjectOptional.get().getMetadata())
                    .spec(SchemaCompatibilityState.SchemaCompatibilityStateSpec.builder()
                        .compatibility(compatibility)
                        .build())
                    .build();

                if (latestSubjectOptional.get().getSpec().getCompatibility().equals(compatibility)) {
                    return Mono.just(HttpResponse.ok(state));
                }

                return schemaService
                    .updateSubjectCompatibility(ns, latestSubjectOptional.get(), compatibility)
                    .map(schemaCompatibility -> {
                        sendEventLog(
                            latestSubjectOptional.get(),
                            ApplyStatus.changed,
                            latestSubjectOptional.get().getSpec().getCompatibility(),
                            compatibility,
                            EMPTY_STRING
                        );

                        return HttpResponse.ok(state);
                    });
            });
    }
}
