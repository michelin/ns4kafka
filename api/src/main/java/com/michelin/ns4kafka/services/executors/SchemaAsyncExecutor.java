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
import java.util.Date;
import java.util.List;
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

        List<Schema> schemas = this.schemaRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(schema -> !schema.getStatus().getPhase().equals(Schema.SchemaPhase.Success))
                .collect(Collectors.toList());

        log.debug("{} schemas found to publish: {} pending, {} failed", schemas.size(),
                schemas.stream().filter(schema -> schema.getStatus().getPhase().equals(Schema.SchemaPhase.Pending)).count(),
                schemas.stream().filter(schema -> schema.getStatus().getPhase().equals(Schema.SchemaPhase.Failed)).count());

        if (!schemas.isEmpty()) {
            log.debug("Schemas {} found on ns4kafka for cluster {} and schema registry {}",
                    schemas.stream().map(schema -> schema.getMetadata().getName()).collect(Collectors.toList()),
                    kafkaAsyncExecutorConfig.getName(), kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl());

            schemas.forEach(this::publishSchema);
        }
    }

    /**
     * Publish given schema to the schema registry of the current managed cluster
     *
     * @param schema The schema to publish
     */
    private void publishSchema(Schema schema) {
        try {
            this.kafkaSchemaRegistryClient.publish(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    schema.getMetadata().getCluster(), schema.getMetadata().getName() + "-value",
                    schema.getSpec());

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
            schema.setStatus(Schema.SchemaStatus.ofSuccess("Schema published"));

            this.schemaRepository.create(schema);
        }
    }
}
