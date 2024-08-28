package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.util.enumation.Kind.SCHEMA;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.schema.Schema;
import com.michelin.ns4kafka.model.schema.SchemaCompatibilityState;
import com.michelin.ns4kafka.model.schema.SchemaList;
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
    public Flux<SchemaList> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        return schemaService.findByWildcardName(getNamespace(namespace), name);
    }

    /**
     * Get the last version of a schema by namespace and subject.
     *
     * @param namespace The namespace
     * @param subject   The subject
     * @return A schema
     * @deprecated use list(String, String name) instead.
     */
    @Get("/{subject}")
    @Deprecated(since = "1.12.0")
    public Mono<Schema> get(String namespace, String subject) {
        Namespace ns = getNamespace(namespace);

        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Mono.empty();
        }

        return schemaService.getLatestSubject(ns, subject);
    }

    /**
     * Publish a schema.
     *
     * @param namespace The namespace
     * @param schema    The schema to create
     * @param dryrun    Does the creation is a dry run
     * @return The created schema
     */
    @Post
    public Mono<HttpResponse<Schema>> apply(String namespace, @Valid @Body Schema schema,
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
                                            sendEventLog(schema, status,
                                                oldSchemas.isEmpty() ? null : oldSchemas.stream()
                                                    .max(Comparator.comparingInt(
                                                        (Schema s) -> s.getSpec().getId())),
                                                schema.getSpec(), "");

                                            return formatHttpResponse(schema, status);
                                        });
                                });
                        }));
            });
    }

    /**
     * Delete all schema versions under the given subject, or a specific version of the schema if specified.
     *
     * @param namespace The namespace
     * @param subject   The subject
     * @param version   The version of the schema to delete
     * @param dryrun    Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{subject}")
    public Mono<HttpResponse<Void>> delete(String namespace,
                                           @PathVariable String subject,
                                           @QueryValue Optional<String> version,
                                           @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate ownership
        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Mono.error(new ResourceValidationException(SCHEMA, subject, invalidOwner(subject)));
        }

        return version
            .map(v -> schemaService.getSubject(ns, subject, v))
            .orElseGet(() -> schemaService.getLatestSubject(ns, subject))
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMap(subjectOptional -> {
                    if (subjectOptional.isEmpty()) {
                        return Mono.just(HttpResponse.notFound());
                    }

                    if (dryrun) {
                        return Mono.just(HttpResponse.noContent());
                    }

                    return (version.isEmpty() ? schemaService.deleteAllVersions(ns, subject) :
                        schemaService.deleteVersion(ns, subject, version.get()))
                            .map(deletedVersionIds -> {
                                Schema deletedSchema = subjectOptional.get();
                                sendEventLog(deletedSchema, ApplyStatus.deleted, deletedSchema.getSpec(), null,
                                    version.map(v -> String.valueOf(deletedVersionIds)).orElseGet(() -> ""));
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
    public Mono<HttpResponse<SchemaCompatibilityState>> config(String namespace, @PathVariable String subject,
                                                               Schema.Compatibility compatibility) {
        Namespace ns = getNamespace(namespace);

        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Mono.error(new ResourceValidationException(SCHEMA, subject, invalidOwner(subject)));
        }

        return schemaService.getLatestSubject(ns, subject)
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
                        sendEventLog(latestSubjectOptional.get(), ApplyStatus.changed,
                            latestSubjectOptional.get().getSpec().getCompatibility(), compatibility, "");

                        return HttpResponse.ok(state);
                    });
            });
    }
}
