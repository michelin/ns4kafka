package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Schema;
import com.michelin.ns4kafka.repositories.SchemaRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaSchemaRepository extends KafkaStore<Schema> implements SchemaRepository {
    /**
     * Constructor
     *
     * @param kafkaTopic The technical topic for schemas
     * @param kafkaProducer The kafka producer for schemas
     */
    public KafkaSchemaRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.schemas") String kafkaTopic,
                                    @KafkaClient("schemas-producer") Producer<String, Schema> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    /**
     * Register the schema to the schemas technical topic
     *
     * @param schema The schema to register
     * @return The registered schema
     */
    @Override
    public Schema create(Schema schema) {
        return this.produce(getMessageKey(schema),schema);
    }

    /**
     * Consume records from the schemas technical topic
     *
     * @param record The record
     */
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.schemas")
    void receive(ConsumerRecord<String, Schema> record) {
        super.receive(record);
    }

    /**
     * Find all schemas for cluster
     *
     * @param cluster The cluster
     * @return A list of schemas
     */
    @Override
    public List<Schema> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(schema -> schema.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

    /**
     * Find a schema by name
     *
     * @param name The name of the schema
     * @return A schema matching the given name
     */
    @Override
    public Optional<Schema> findByName(String name) {
        return getKafkaStore().values().stream()
                .filter(schema -> schema.getMetadata().getName().equals(name))
                .findFirst();
    }

    /**
     * Build a key according to a given schema resource
     *
     * @param schema The schema resource
     * @return The key
     */
    @Override
    String getMessageKey(Schema schema) {
        return schema.getMetadata().getNamespace() + "/" + schema.getMetadata().getName();
    }
}
