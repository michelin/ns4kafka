package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import java.util.List;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaConnectorRepository extends KafkaStore<Connector> implements ConnectorRepository {
    public KafkaConnectorRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.connectors") String kafkaTopic,
                                    @KafkaClient("connectors-producer") Producer<String, Connector> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(Connector connector) {
        return connector.getMetadata().getNamespace() + "/" + connector.getMetadata().getName();
    }

    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.connectors")
    void receive(ConsumerRecord<String, Connector> record) {
        super.receive(record);
    }

    /**
     * Create a given connector
     * @param connector The connector to create
     * @return The created connector
     */
    @Override
    public Connector create(Connector connector) {
        return this.produce(getMessageKey(connector),connector);
    }

    /**
     * Delete a given connector
     * @param connector The connector to delete
     */
    @Override
    public void delete(Connector connector) {
        this.produce(getMessageKey(connector),null);
    }

    /**
     * Find all connectors by cluster
     * @param cluster The cluster
     * @return The list of connectors
     */
    @Override
    public List<Connector> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(connector -> connector.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }
}
