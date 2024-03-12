package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

/**
 * Kafka Namespace repository.
 */
@Singleton
@KafkaListener(
    offsetReset = OffsetReset.EARLIEST,
    groupId = "${ns4kafka.store.kafka.group-id}",
    offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaNamespaceRepository extends KafkaStore<Namespace> implements NamespaceRepository {

    public KafkaNamespaceRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.namespaces") String kafkaTopic,
                                    @KafkaClient("namespace-producer") Producer<String, Namespace> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(Namespace namespace) {
        return namespace.getMetadata().getName();
    }

    @Override
    public Namespace createNamespace(Namespace namespace) {
        return produce(getMessageKey(namespace), namespace);
    }

    @Override
    public void delete(Namespace namespace) {
        produce(getMessageKey(namespace), null);
    }

    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.namespaces")
    void receive(ConsumerRecord<String, Namespace> message) {
        super.receive(message);
    }

    @Override
    public List<Namespace> findAllForCluster(String cluster) {
        return getKafkaStore().values()
            .stream()
            .filter(namespace -> namespace.getMetadata().getCluster().equals(cluster))
            .toList();
    }

    @Override
    public Optional<Namespace> findByName(String namespace) {
        return getKafkaStore().values()
            .stream()
            .filter(ns -> ns.getMetadata().getName().equals(namespace))
            .findFirst();
    }

}
