package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.repositories.SchemaRepository;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import io.micronaut.context.annotation.EachBean;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class SchemaAsyncExecutor {
    /**
     * Configuration
     */
    @Inject
    KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    /**
     * Schema repository
     */
    @Inject
    SchemaRepository schemaRepository;

    /**
     * Kafka schema registry client
     */
    @Inject
    KafkaSchemaRegistryClient kafkaSchemaRegistryClient;

    /**
     * Run method
     */
    public void run() {
        log.debug("Starting schema collection for cluster {} and schema registry {}", kafkaAsyncExecutorConfig.getName(),
                kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl());

        List<Schema> schemasToPublish = this.schemaRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(schema -> Arrays.asList(Schema.SchemaPhase.Pending, Schema.SchemaPhase.Failed).contains(schema.getStatus().getPhase()))
                .collect(Collectors.toList());

        log.debug("{} schemas found to publish ({} pending, {} failed)", schemasToPublish.size(),
                schemasToPublish.stream().filter(schema -> schema.getStatus().getPhase().equals(Schema.SchemaPhase.Pending)).count(),
                schemasToPublish.stream().filter(schema -> schema.getStatus().getPhase().equals(Schema.SchemaPhase.Failed)).count());

        schemasToPublish.forEach(this::publishSchema);

        List<Schema> schemasToSoftDelete = this.schemaRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(schema -> Arrays.asList(Schema.SchemaPhase.PendingSoftDeletion, Schema.SchemaPhase.FailedSoftDeletion)
                        .contains(schema.getStatus().getPhase()))
                .collect(Collectors.toList());

        log.debug("{} schemas found to soft delete", schemasToSoftDelete.size());

        schemasToSoftDelete.forEach(this::softDeleteSchema);

        List<Schema> schemasToHardDelete = this.schemaRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(schema -> Arrays.asList(Schema.SchemaPhase.PendingHardDeletion, Schema.SchemaPhase.FailedHardDeletion)
                        .contains(schema.getStatus().getPhase()))
                .collect(Collectors.toList());

        log.debug("{} schemas found to hard delete", schemasToHardDelete.size());

        schemasToHardDelete.forEach(this::hardDeleteSchema);
    }

    /**
     * Publish given schema to the schema registry of the current managed cluster
     *
     * @param schema The schema to publish
     */
    private void publishSchema(Schema schema) {
        try {
            this.kafkaSchemaRegistryClient.publish(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    schema.getMetadata().getCluster(), schema.getMetadata().getName(),
                    schema.getSpec().getContent());

            log.info("Success deploying schema [{}] on schema registry [{}] of kafka cluster [{}]",
                    schema.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName());

            schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            schema.setStatus(Schema.SchemaStatus.ofSuccess("Schema published"));

            this.schemaRepository.create(schema);
        } catch (Exception e) {
            log.error("Error deploying schema [{}] on schema registry [{}] of kafka cluster [{}]",
                    schema.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName(), e);

            schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            schema.setStatus(Schema.SchemaStatus.ofFailed("Schema failed"));

            this.schemaRepository.create(schema);
        }
    }

    /**
     * Soft delete given schema from the schema registry of the current managed cluster
     *
     * @param schema The schema to soft delete
     */
    private void softDeleteSchema(Schema schema) {
        try {
            this.kafkaSchemaRegistryClient.deleteBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    schema.getMetadata().getCluster(), schema.getMetadata().getName(),
                    false);

            log.info("Success soft deleting schema [{}] from schema registry [{}] of kafka cluster [{}]",
                    schema.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName());

            schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            schema.setStatus(Schema.SchemaStatus.ofSoftDeleted());

            this.schemaRepository.create(schema);
        } catch (Exception e) {
            log.error("Error soft deleting schema [{}] on schema registry [{}] of kafka cluster [{}]",
                    schema.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName(), e);

            schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            schema.setStatus(Schema.SchemaStatus.ofFailedSoftDeletion());

            this.schemaRepository.create(schema);
        }
    }

    /**
     * Hard delete given schema from the schema registry of the current managed cluster
     *
     * @param schema The schema to hard delete
     */
    private void hardDeleteSchema(Schema schema) {
        try {
            this.kafkaSchemaRegistryClient.deleteBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    schema.getMetadata().getCluster(), schema.getMetadata().getName(),
                    true);

            log.info("Success hard deleting schema [{}] from schema registry [{}] of kafka cluster [{}]",
                    schema.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName());

            schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            schema.setStatus(Schema.SchemaStatus.ofSoftDeleted());

            this.schemaRepository.delete(schema);
        } catch (Exception e) {
            log.error("Error hard deleting schema [{}] on schema registry [{}] of kafka cluster [{}]",
                    schema.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName(), e);

            schema.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            schema.setStatus(Schema.SchemaStatus.ofFailedHardDeletion());

            this.schemaRepository.create(schema);
        }
    }
}
