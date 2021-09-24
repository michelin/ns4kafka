package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.services.KafkaConnectService;
import com.michelin.ns4kafka.services.SchemaService;
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
     * Publish a schema to the schema registry
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

        schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        schema.getMetadata().setCluster(retrievedNamespace.getMetadata().getCluster());
        schema.getMetadata().setNamespace(retrievedNamespace.getMetadata().getName());
        schema.setStatus(Schema.SchemaStatus.ofPending());

        if (dryrun) {
            return formatHttpResponse(schema, ApplyStatus.created);
        }

        return formatHttpResponse(this.schemaService.create(schema), ApplyStatus.created);
    }
}
