package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.repositories.SchemaRepository;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Slf4j
@Singleton
public class SchemaService {
    /**
     * ACLs service
     */
    @Inject
    AccessControlEntryService accessControlEntryService;

    /**
     * Schema repository
     */
    @Inject
    SchemaRepository schemaRepository;

    /**
     * Publish a schema to the schemas technical topic
     *
     * @param schema The schema to publish
     * @return The created schema
     */
    public Schema create(Schema schema) {
        return this.schemaRepository.create(schema);
    }

    /**
     * Find a schema by name
     *
     * @param name The name of the schema
     * @return A schema matching the given name
     */
    public Optional<Schema> findByName(String name) {
        return this.schemaRepository.findByName(name);
    }

    /**
     * Does the namespace is owner of the given schema
     *
     * @param namespace The namespace
     * @param schemaName The name of the schema
     * @return true if it's owner, false otherwise
     */
    public boolean isNamespaceOwnerOfSchema(Namespace namespace, String schemaName) {
        return this.accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(), AccessControlEntry.ResourceType.SCHEMA,
                schemaName);
    }
}
