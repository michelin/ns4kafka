package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.services.SchemaService;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibilityConfig;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibilityCheck;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;

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
     * Schema service
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
        return this.schemaService.findAllForNamespace(getNamespace(namespace));
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

        if (!this.schemaService.isNamespaceOwnerOfSchema(retrievedNamespace, subject)) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this schema"), AccessControlEntry.ResourceType.SCHEMA.toString(),
                    subject);
        }

        return this.schemaService.findByName(getNamespace(namespace), subject);
    }

    /**
     * Publish a schema to the schemas technical topic
     *
     * @param namespace The namespace
     * @param schema The schema to create
     * @param dryrun Does the creation is a dry run
     * @return The created schema
     */
    @Post
    public HttpResponse<Schema> apply(String namespace, @Valid @Body Schema schema, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSchema(retrievedNamespace, schema.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid prefix " + schema.getMetadata().getName() +
                    " : namespace not owner of this schema"), schema.getKind(), schema.getMetadata().getName());
        }

        // If a compatibility is specified in the yml, apply it
        if (schema.getSpec().getCompatibility() != null && StringUtils.isNotBlank(schema.getSpec().getCompatibility().toString())) {
            this.schemaService.updateSchemaCompatibility(retrievedNamespace.getMetadata().getCluster(), schema,
                    SchemaCompatibilityConfig.builder()
                            .compatibility(schema.getSpec().getCompatibility())
                            .build());
        }

        // Check the current compatibility of the new schema
        List<String> errorsValidateSchemaCompatibility = this.schemaService
                .validateSchemaCompatibility(retrievedNamespace.getMetadata().getCluster(), schema);

        if (!errorsValidateSchemaCompatibility.isEmpty()) {
            throw new ResourceValidationException(errorsValidateSchemaCompatibility, schema.getKind(), schema.getMetadata().getName());
        }

        schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        schema.getMetadata().setCluster(retrievedNamespace.getMetadata().getCluster());
        schema.getMetadata().setNamespace(retrievedNamespace.getMetadata().getName());
        schema.setStatus(Schema.SchemaStatus.ofPending());

        Optional<Schema> existingSchema = this.schemaService.findByName(retrievedNamespace,
                schema.getMetadata().getName());

        // Do nothing if schemas are equals and existing schema is not in deletion
        // If the schema is in deletion, we want to publish it again even if no change is detected
        if (existingSchema.isPresent() && !Schema.SchemaStatus.inDeletion(existingSchema.get().getStatus().getPhase())
                && existingSchema.get().equals(schema)) {
            return formatHttpResponse(existingSchema.get(), ApplyStatus.unchanged);
        }

        if (dryrun) {
            return formatHttpResponse(schema, ApplyStatus.created);
        }

        ApplyStatus status = existingSchema.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        super.sendEventLog(schema.getKind(), schema.getMetadata(), status,
                existingSchema.<Object>map(Schema::getSpec).orElse(null), schema.getSpec());

        return formatHttpResponse(this.schemaService.create(schema), status);
    }

    /**
     * Hard/soft delete all the schemas under the given subject
     * @param namespace The current namespace
     * @param subject The current subject to delete
     * @param dryrun Run in dry mode or not
     * @param hard Run in hard mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{subject}")
    public HttpResponse<Void> deleteBySubject(String namespace, @PathVariable String subject,
                                              @QueryValue(defaultValue = "false") boolean dryrun,
                                              @QueryValue(defaultValue = "false") boolean hard) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSchema(retrievedNamespace, subject)) {
            throw new ResourceValidationException(List.of("Invalid prefix " + subject +
                    " : namespace not owner of this schema"), AccessControlEntry.ResourceType.SCHEMA.toString(),
                    subject);
        }

        Optional<Schema> existingSchema = this.schemaService.findByName(retrievedNamespace,
                subject);

        if (existingSchema.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        Schema schema = existingSchema.get();
        schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        schema.setStatus(hard ? Schema.SchemaStatus.ofPendingHardDeletion(): Schema.SchemaStatus.ofPendingSoftDeletion());

        super.sendEventLog(schema.getKind(), schema.getMetadata(), hard ? ApplyStatus.changed : ApplyStatus.deleted,
                existingSchema.<Object>map(Schema::getSpec).orElse(null), schema.getSpec());

        this.schemaService.create(schema);

        return HttpResponse.noContent();
    }
}
