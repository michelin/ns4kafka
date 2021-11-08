package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.services.SchemaService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Tag(name = "Schemas")
@Controller(value = "/api/namespaces/{namespace}/schemas")
@ExecuteOn(TaskExecutors.IO)
public class SchemaController extends NamespacedResourceController {

    /**
     * Subject service
     */
    @Inject
    SchemaService schemaService;

    /**
     * Get all the schemas within a given namespace
     *
     * @param namespace The namespace
     * @return A list of schemas
     */
    @Get
    public List<Schema> list(String namespace) {
        Namespace ns = getNamespace(namespace);
        return this.schemaService.findAllForNamespace(ns);
    }

    /**
     * Get the last version of a schema by namespace and subject
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return A schema
     */
    @Get("/{subject}")
    public Optional<Schema> get(String namespace, String subject) {
        Namespace ns = getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            return Optional.empty();
        }

        return this.schemaService.getLatestSubject(ns, subject);
    }

    /**
     * Publish a schema
     *
     * @param namespace The namespace
     * @param schema The schema to create
     * @param dryrun Does the creation is a dry run
     * @return The created subject
     */
    @Post
    public HttpResponse<Schema> apply(String namespace, @Valid @Body Schema schema, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate TopicNameStrategy
        // https://github.com/confluentinc/schema-registry/blob/master/schema-serializer/src/main/java/io/confluent/kafka/serializers/subject/TopicNameStrategy.java
        if(!schema.getMetadata().getName().endsWith("-key") && !schema.getMetadata().getName().endsWith("-value")){
            throw new ResourceValidationException(List.of("Invalid value " + schema.getMetadata().getName() +
                    " for name: : subject must end with -key or -value"), schema.getKind(), schema.getMetadata().getName());
        }

        // Validate ownership
        if (!this.schemaService.isNamespaceOwnerOfSubject(ns, schema.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid value " + schema.getMetadata().getName() +
                    " for name: : namespace not OWNER of underlying topic"), schema.getKind(), schema.getMetadata().getName());
        }

        // Validate compatibility
        List<String> validationErrors = this.schemaService.validateSchemaCompatibility(ns.getMetadata().getCluster(), schema);

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, schema.getKind(), schema.getMetadata().getName());
        }

        Optional<Schema> existingSchemaOptional = this.schemaService.getLatestSubject(ns, schema.getMetadata().getName());

        ApplyStatus status = existingSchemaOptional.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        if (dryrun) {
            return this.formatHttpResponse(schema, status);
        }

        Optional<Schema> schemaOptional = this.schemaService.register(ns, schema);
        status = schemaOptional.isPresent() ? status : ApplyStatus.unchanged;
        Schema appliedSchema = schemaOptional.orElse(null);

        return this.formatHttpResponse(appliedSchema, status);
    }

    /**
     * Delete all schemas under the given subject
     *
     * @param namespace The current namespace
     * @param subject The current subject to delete
     * @param dryrun Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{subject}")
    public HttpResponse<Void> deleteSubject(String namespace, @PathVariable String subject,
                                              @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSubject(retrievedNamespace, subject)) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this subject"), AccessControlEntry.ResourceType.SCHEMA.toString(), subject);
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        this.schemaService.deleteSubject(retrievedNamespace, subject);

        return HttpResponse.noContent();
    }

    /**
     * Update the compatibility of a subject
     *
     * @param namespace The namespace
     * @param subject The subject to update
     * @param compatibility The compatibility to apply
     * @param dryrun Dry run mode
     * @return The last updated schema
     */
    @Post("/{subject}/compatibility")
    public HttpResponse<Optional<Schema>> compatibility(String namespace, @PathVariable String subject, @Valid @Body Map<String, Schema.Compatibility> compatibility,
                                                        @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSubject(ns, subject)) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this subject"), AccessControlEntry.ResourceType.SCHEMA.toString(), subject);
        }

        Optional<Schema> existingSchemaOptional = this.schemaService
                .getBySubjectAndVersion(retrievedNamespace, subject, SchemaController.SCHEMA_LATEST_VERSION);

        // Subject not existing, no compatibility to update
        if (existingSchemaOptional.isEmpty()) {
            return this.formatHttpResponse(Optional.empty(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingSchemaOptional.get().getSpec().getCompatibility()
                .equals(compatibility.get(SchemaController.COMPATIBILITY_KEY)) ? ApplyStatus.unchanged : ApplyStatus.changed;

        // Subject existing, but given compatibility equals the current one
        if (status.equals(ApplyStatus.unchanged)) {
            return this.formatHttpResponse(existingSchemaOptional, status);
        }

        // Subject existing, update the compatibility (dry mode)
        if (dryrun) {
            return this.formatHttpResponse(this.schemaService.getBySubjectAndVersion(retrievedNamespace, subject, SchemaController.SCHEMA_LATEST_VERSION), status);
        }

        // Subject existing, update the compatibility
        return this.formatHttpResponse(this.schemaService.updateSubjectCompatibility(retrievedNamespace, subject, compatibility.get(SchemaController.COMPATIBILITY_KEY)), ApplyStatus.changed);
    }
}
