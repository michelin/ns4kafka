package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.schema.Schema;
import com.michelin.ns4kafka.models.schema.SchemaCompatibilityState;
import com.michelin.ns4kafka.models.schema.SchemaList;
import com.michelin.ns4kafka.services.SchemaService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Tag(name = "Schemas")
@Controller(value = "/api/namespaces/{namespace}/schemas")
@ExecuteOn(TaskExecutors.IO)
public class SchemaController extends NamespacedResourceController {
    /**
     * The schema service
     */
    @Inject
    SchemaService schemaService;

    /**
     * Get all the schemas by namespace
     * @param namespace The namespace
     * @return A list of schemas
     */
    @Get
    public Single<List<SchemaList>> list(String namespace) {
        Namespace ns = getNamespace(namespace);
        return schemaService.findAllForNamespace(ns);
    }

    /**
     * Get the last version of a schema by namespace and subject
     * @param namespace The namespace
     * @param subject   The subject
     * @return A schema
     */
    @Get("/{subject}")
    public Maybe<Schema> get(String namespace, String subject) {
        Namespace ns = getNamespace(namespace);

        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Maybe.empty();
        }

        return schemaService.getLatestSubject(ns, subject);
    }

    /**
     * Publish a schema
     * @param namespace The namespace
     * @param schema    The schema to create
     * @param dryrun    Does the creation is a dry run
     * @return The created schema
     */
    @Post
    public Single<HttpResponse<Schema>> apply(String namespace, @Valid @Body Schema schema, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate TopicNameStrategy
        // https://github.com/confluentinc/schema-registry/blob/master/schema-serializer/src/main/java/io/confluent/kafka/serializers/subject/TopicNameStrategy.java
        if (!schema.getMetadata().getName().endsWith("-key") && !schema.getMetadata().getName().endsWith("-value")) {
            return Single.error(new ResourceValidationException(List.of("Invalid value " + schema.getMetadata().getName() +
                    " for name: subject must end with -key or -value"), schema.getKind(), schema.getMetadata().getName()));
        }

        // Validate ownership
        if (!schemaService.isNamespaceOwnerOfSubject(ns, schema.getMetadata().getName())) {
            return Single.error(new ResourceValidationException(List.of(String.format("Namespace not owner of this schema %s.",
                    schema.getMetadata().getName())), schema.getKind(), schema.getMetadata().getName()));
        }

        return schemaService
                .validateSchemaCompatibility(ns.getMetadata().getCluster(), schema)
                .flatMap(validationErrors -> {
                    if (!validationErrors.isEmpty()) {
                        return Single.error(new ResourceValidationException(validationErrors, schema.getKind(), schema.getMetadata().getName()));
                    }

                    if (dryrun) {
                        // Cannot compute the "apply" status before the registration
                        return Single.just(HttpResponse.ok(schema));
                    }

                    return schemaService
                            .getLatestSubject(ns, schema.getMetadata().getName())
                            .map(Optional::of)
                            .defaultIfEmpty(Optional.empty())
                            .flatMapSingle(latestSubjectOptional -> schemaService
                                    .register(ns, schema)
                                    .map(id -> {
                                        ApplyStatus status;

                                        schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                                        schema.getMetadata().setCluster(ns.getMetadata().getCluster());
                                        schema.getMetadata().setNamespace(ns.getMetadata().getName());

                                        if (latestSubjectOptional.isEmpty()) {
                                            status = ApplyStatus.created;
                                            sendEventLog(schema.getKind(), schema.getMetadata(), status, null, schema.getSpec());
                                        } else if (!id.equals(latestSubjectOptional.get().getSpec().getId())) {
                                            status = ApplyStatus.changed;
                                            sendEventLog(schema.getKind(), schema.getMetadata(), status, latestSubjectOptional.get().getSpec(),
                                                    schema.getSpec());
                                        } else {
                                            status = ApplyStatus.unchanged;
                                        }

                                        return formatHttpResponse(schema, status);
                                    }));
                });
    }

    /**
     * Delete all schemas under the given subject
     * @param namespace The current namespace
     * @param subject   The current subject to delete
     * @param dryrun    Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{subject}")
    public Single<HttpResponse<Void>> deleteSubject(String namespace, @PathVariable String subject,
                                                    @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate ownership
        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Single.error(new ResourceValidationException(List.of(String.format("Namespace not owner of this schema %s.", subject)),
                    AccessControlEntry.ResourceType.SCHEMA.toString(), subject));
        }

       return schemaService.getLatestSubject(ns, subject)
               .map(Optional::of)
               .defaultIfEmpty(Optional.empty())
               .flatMapSingle(latestSubjectOptional -> {
                   if (latestSubjectOptional.isEmpty()) {
                       return Single.just(HttpResponse.notFound());
                   }

                   if (dryrun) {
                       return Single.just(HttpResponse.noContent());
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
     * Update the compatibility of a subject
     * @param namespace     The namespace
     * @param subject       The subject to update
     * @param compatibility The compatibility to apply
     * @return A schema compatibility state
     */
    @Post("/{subject}/config")
    public Single<HttpResponse<SchemaCompatibilityState>> config(String namespace, @PathVariable String subject, Schema.Compatibility compatibility) {
        Namespace ns = getNamespace(namespace);

        if (!schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Single.error(new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this subject"), AccessControlEntry.ResourceType.SCHEMA.toString(), subject));
        }

        return schemaService.getLatestSubject(ns, subject)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMapSingle(latestSubjectOptional -> {
                    if (latestSubjectOptional.isEmpty()) {
                        return Single.just(HttpResponse.notFound());
                    }

                    SchemaCompatibilityState state = SchemaCompatibilityState.builder()
                            .metadata(latestSubjectOptional.get().getMetadata())
                            .spec(SchemaCompatibilityState.SchemaCompatibilityStateSpec.builder()
                                    .compatibility(compatibility)
                                    .build())
                            .build();

                    if (latestSubjectOptional.get().getSpec().getCompatibility().equals(compatibility)) {
                        return Single.just(HttpResponse.ok(state));
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
