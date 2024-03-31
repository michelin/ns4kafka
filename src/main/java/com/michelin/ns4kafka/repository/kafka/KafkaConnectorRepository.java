package com.michelin.ns4kafka.repository.kafka;

import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.repository.ConnectorRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/**
 * Kafka Connector repository.
 */
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
    void receive(ConsumerRecord<String, Connector> message) {
        super.receive(message);
    }

    /**
     * Create a given connector.
     *
     * @param connector The connector to create
     * @return The created connector
     */
    @Override
    public Connector create(Connector connector) {
        return this.produce(getMessageKey(connector), connector);
    }

    /**
     * Delete a given connector.
     *
     * @param connector The connector to delete
     */
    @Override
    public void delete(Connector connector) {
        this.produce(getMessageKey(connector), null);
    }

    /**
     * Find all connectors by cluster.
     *
     * @param cluster The cluster
     * @return The list of connectors
     */
    @Override
    public List<Connector> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
            .filter(connector -> connector.getMetadata().getCluster().equals(cluster))
            .toList();
    }
}
