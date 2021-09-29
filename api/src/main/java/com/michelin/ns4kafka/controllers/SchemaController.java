package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.services.KafkaConnectService;
import com.michelin.ns4kafka.services.SchemaService;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibility;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
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
    public Optional<Schema> getByNamespaceAndSubject(String namespace, String subject) {
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
    @Post("{?dryrun}")
    public HttpResponse<Schema> apply(String namespace, @Valid @Body Schema schema, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace retrievedNamespace = super.getNamespace(namespace);

        if (!this.schemaService.isNamespaceOwnerOfSchema(retrievedNamespace, schema.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid prefix " + schema.getMetadata().getName() +
                    " : namespace not owner of this schema"), schema.getKind(), schema.getMetadata().getName());
        }

        SchemaCompatibility schemaCompatibility = this.schemaService.validateSchemaCompatibility(retrievedNamespace.getMetadata().getCluster(),
                schema);

        if (schemaCompatibility != null && !schemaCompatibility.isCompatible()) {
            throw new ResourceValidationException(List.of("The schema registry rejected the given schema for compatibility reason"),
                    schema.getKind(), schema.getMetadata().getName());
        }

        schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        schema.getMetadata().setCluster(retrievedNamespace.getMetadata().getCluster());
        schema.getMetadata().setNamespace(retrievedNamespace.getMetadata().getName());
        schema.setStatus(Schema.SchemaStatus.ofPending());

        Optional<Schema> existingSchema = this.schemaService.findByName(retrievedNamespace,
                schema.getMetadata().getName());

        if (existingSchema.isPresent() && existingSchema.get().equals(schema)) {
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
}
