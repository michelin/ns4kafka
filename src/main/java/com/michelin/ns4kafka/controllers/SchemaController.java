package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaCompatibilityState;
import com.michelin.ns4kafka.models.schema.SchemaList;
import com.michelin.ns4kafka.services.SchemaService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
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
     * List schemas by namespace.
     *
     * @param namespace The namespace
     * @return A list of schemas
     */
    @Get
    public Flux<SchemaList> list(String namespace) {
        return schemaService.findAllForNamespace(getNamespace(namespace));
    }

    /**
     * Get the last version of a schema by namespace and subject.
     *
     * @param namespace The namespace
     * @param subject   The subject
     * @return A schema
     */
    @Get("/{subject}")
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

        // Validate TopicNameStrategy
        // https://github.com/confluentinc/schema-registry/blob/master/schema-serializer/src/main/java/io/confluent/kafka/serializers/subject/TopicNameStrategy.java
        if (!schema.getMetadata().getName().endsWith("-key") && !schema.getMetadata().getName().endsWith("-value")) {
            return Mono.error(
                new ResourceValidationException(List.of("Invalid value " + schema.getMetadata().getName()
                    + " for name: subject must end with -key or -value"), schema.getKind(),
                    schema.getMetadata().getName()));
        }

        // Validate ownership
        if (!schemaService.isNamespaceOwnerOfSubject(ns, schema.getMetadata().getName())) {
            return Mono.error(
                new ResourceValidationException(List.of(String.format("Namespace not owner of this schema %s.",
                    schema.getMetadata().getName())), schema.getKind(), schema.getMetadata().getName()));
        }

        return schemaService
            .validateSchemaCompatibility(ns.getMetadata().getCluster(), schema)
            .flatMap(validationErrors -> {
                if (!validationErrors.isEmpty()) {
                    return Mono.error(new ResourceValidationException(validationErrors, schema.getKind(),
                        schema.getMetadata().getName()));
                }

                return schemaService
                    .getLatestSubject(ns, schema.getMetadata().getName())
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty())
                    .flatMap(latestSubjectOptional -> {
                        schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                        schema.getMetadata().setCluster(ns.getMetadata().getCluster());
                        schema.getMetadata().setNamespace(ns.getMetadata().getName());
                        latestSubjectOptional.ifPresent(
                            value -> schema.getSpec().setCompatibility(value.getSpec().getCompatibility()));

                        if (latestSubjectOptional.isPresent()) {
                            var newSchema = new AvroSchema(schema.getSpec().getSchema());
                            var actualSchema = new AvroSchema(latestSubjectOptional.get().getSpec().getSchema());

                            if (newSchema.canonicalString().equals(actualSchema.canonicalString())) {
                                return Mono.just(formatHttpResponse(schema, ApplyStatus.unchanged));
                            }
                         }

                        if (dryrun) {
                            return Mono.just(formatHttpResponse(schema,
                                latestSubjectOptional.isPresent() ? ApplyStatus.changed : ApplyStatus.created));
                        }

                        return schemaService
                            .register(ns, schema)
                            .map(id -> {
                                ApplyStatus status;

                                if (latestSubjectOptional.isEmpty()) {
                                    status = ApplyStatus.created;
                                    sendEventLog(schema.getKind(), schema.getMetadata(), status, null,
                                        schema.getSpec());
                                } else if (id > latestSubjectOptional.get().getSpec().getId()) {
                                    status = ApplyStatus.changed;
                                    sendEventLog(schema.getKind(), schema.getMetadata(), status,
                                        latestSubjectOptional.get().getSpec(),
                                        schema.getSpec());
                                } else {
                                    status = ApplyStatus.unchanged;
                                }

                                return formatHttpResponse(schema, status);
                            });
                    });
            });
    }

    /**
     * Delete all schemas under the given subject.
     *
     * @param namespace The current namespace
     * @param subject   The current subject to delete
     * @param dryrun    Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{subject}")
    public Mono<HttpResponse<Void>> deleteSubject(String namespace, @PathVariable String subject,
                                                  @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate ownership
        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Mono.error(new ResourceValidationException(
                List.of(String.format("Namespace not owner of this schema %s.", subject)),
                AccessControlEntry.ResourceType.SCHEMA.toString(), subject));
        }

        return schemaService.getLatestSubject(ns, subject)
            .map(Optional::of)
            .defaultIfEmpty(Optional.empty())
            .flatMap(latestSubjectOptional -> {
                if (latestSubjectOptional.isEmpty()) {
                    return Mono.just(HttpResponse.notFound());
                }

                if (dryrun) {
                    return Mono.just(HttpResponse.noContent());
                }

                Schema schemaToDelete = latestSubjectOptional.get();
                sendEventLog(schemaToDelete.getKind(),
                    schemaToDelete.getMetadata(),
                    ApplyStatus.deleted,
                    schemaToDelete.getSpec(),
                    null);

                return schemaService
                    .deleteSubject(ns, subject)
                    .map(deletedSchemaIds -> HttpResponse.noContent());
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
            return Mono.error(new ResourceValidationException(List.of("Invalid prefix " + subject
                + " : namespace not owner of this subject"),
                AccessControlEntry.ResourceType.SCHEMA.toString(), subject));
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
                        sendEventLog("SchemaCompatibilityState",
                            latestSubjectOptional.get().getMetadata(),
                            ApplyStatus.changed,
                            latestSubjectOptional.get().getSpec().getCompatibility(),
                            compatibility);

                        return HttpResponse.ok(state);
                    });
            });
    }
}
