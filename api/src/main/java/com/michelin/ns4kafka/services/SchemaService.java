package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.repositories.SchemaRepository;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibilityConfig;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaCompatibilityCheck;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
     * Schema Registry client
     */
    @Inject
    KafkaSchemaRegistryClient kafkaSchemaRegistryClient;

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
     * @param namespace The namespace
     * @param name The name of the schema
     * @return A schema matching the given name
     */
    public Optional<Schema> findByName(Namespace namespace, String name) {
        return this.findAllForNamespace(namespace)
                .stream()
                .filter(schema -> schema.getMetadata().getName().equals(name))
                .findFirst();
    }

    /**
     * Find all schemas on a given namespace
     *
     * @param namespace The namespace used to research the schemas
     * @return A list of schemas
     */
    public List<Schema> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = this.accessControlEntryService.findAllGrantedToNamespace(namespace);

        return this.schemaRepository.findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(schema -> acls.stream().anyMatch(accessControlEntry -> {
                    // need to check accessControlEntry.Permission, we want OWNER
                    if (accessControlEntry.getSpec().getPermission() != AccessControlEntry.Permission.OWNER) {
                        return false;
                    }

                    if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.SCHEMA) {
                        switch (accessControlEntry.getSpec().getResourcePatternType()) {
                            case PREFIXED:
                                return schema.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                            case LITERAL:
                                return schema.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                        }
                    }

                    return false;
                }))
                .collect(Collectors.toList());
    }

    /**
     * Validate the schema compatibility against the Schema Registry
     *
     * @param cluster The cluster linked with the Schema Registry to call
     * @param schema The schema to update the compatibility
     * @param schemaCompatibilityConfig The compatibility to apply
     */
    public void updateSchemaCompatibility(String cluster, Schema schema, SchemaCompatibilityConfig schemaCompatibilityConfig) {
        this.kafkaSchemaRegistryClient.updateSchemaCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, schema.getMetadata().getName(), schemaCompatibilityConfig);
    }

    /**
     * Validate the schema compatibility against the Schema Registry
     *
     * @param cluster The cluster linked with the Schema Registry to call
     * @param schema The schema to validate
     * @return A list of errors
     */
    public List<String> validateSchemaCompatibility(String cluster, Schema schema) {
        HttpResponse<SchemaCompatibilityCheck> response = this.kafkaSchemaRegistryClient.validateSchemaCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, schema.getMetadata().getName(),schema.getSpec().getContent());

        if (response.getBody().isPresent() && !response.getBody().get().isCompatible()) {
            return List.of("The schema registry rejected the given schema for compatibility reason");
        }

        return Collections.emptyList();
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
