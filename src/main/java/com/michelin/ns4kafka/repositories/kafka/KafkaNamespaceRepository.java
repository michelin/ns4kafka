package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Singleton;
import java.util.Collection;
import java.util.Optional;


@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaNamespaceRepository extends KafkaStore<Namespace> implements NamespaceRepository {

    public KafkaNamespaceRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.namespaces") String kafkaTopic,
                                    @KafkaClient("namespace-producer") Producer<String, Namespace> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.namespaces")
    void receive(ConsumerRecord<String, Namespace> record) {
        super.receive(record);
    }


    @Override
    public Collection<Namespace> findAll() {
        return getKafkaStore().values();
    }

    @Override
    public Namespace createNamespace(Namespace namespace) {
        return produce(namespace.getName(),namespace);
    }

    @Override
    public Optional<Namespace> findByName(String namespace) {
        return findAll()
                .stream()
                .filter(ns -> ns.getName().equals(namespace))
                .findFirst();
    }

}
