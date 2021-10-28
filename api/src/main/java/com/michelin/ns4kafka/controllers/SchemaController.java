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
    public List<Schema> getAllByNamespace(String namespace) {
        return this.schemaService.getAllByNamespace(getNamespace(namespace));
    }

    /**
     * Get the last version of a schema by namespace and subject
     *
     * @param namespace The namespace
     * @param subject The subject
     * @return A schema
     */
    @Get("/{subject}")
    public Optional<Schema> getByNamespaceAndSubject(String namespace, @PathVariable String subject) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSubject(retrievedNamespace, subject)) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this subject"), AccessControlEntry.ResourceType.SCHEMA.toString(),
                    subject);
        }

        return this.schemaService.getBySubjectAndVersion(retrievedNamespace, subject, "latest");
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
    public HttpResponse<Optional<Schema>> apply(String namespace, @Valid @Body Schema schema, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSubject(retrievedNamespace, schema.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid prefix " + schema.getMetadata().getName() +
                    " : namespace not owner of this subject"), schema.getKind(), schema.getMetadata().getName());
        }

        if (dryrun) {
            List<String> errorsValidateSubjectCompatibility = this.schemaService
                    .validateSchemaCompatibility(retrievedNamespace.getMetadata().getCluster(), schema);

            if (!errorsValidateSubjectCompatibility.isEmpty()) {
                throw new ResourceValidationException(errorsValidateSubjectCompatibility, schema.getKind(), schema.getMetadata().getName());
            }

            return this.formatHttpResponse(Optional.of(schema), ApplyStatus.created);
        }

        return this.formatHttpResponse(this.schemaService.register(retrievedNamespace, schema), ApplyStatus.created);
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
     * Update the compatibility of a schema
     *
     * @param namespace The namespace
     * @param schema The schema to create
     * @return The updated subject
     */
    @Post("/{subject}/compatibility")
    public HttpResponse<Optional<Schema>> compatibility(String namespace, @PathVariable String subject, @Valid @Body Map<String, Schema.Compatibility> compatibility,
                                                        @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSubject(retrievedNamespace, subject)) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this subject"), AccessControlEntry.ResourceType.SCHEMA.toString(), subject);
        }

        if (dryrun) {
            return this.formatHttpResponse(this.schemaService.getBySubjectAndVersion(retrievedNamespace, subject, "latest"), ApplyStatus.unchanged);
        }

        return this.formatHttpResponse(this.schemaService.updateSubjectCompatibility(retrievedNamespace, subject, compatibility.get("compatibility")), ApplyStatus.changed);
    }
}
