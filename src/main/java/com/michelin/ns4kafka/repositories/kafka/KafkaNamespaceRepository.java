package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import io.micronaut.security.utils.SecurityService;
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

    private SecurityService securityService;

    public KafkaNamespaceRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.namespaces") String topic,
                                    @KafkaClient("namespace-producer") Producer<String, Namespace> kafkaProducer,
                                    SecurityService securityService) {
        super(topic, kafkaProducer);
        this.securityService=securityService;
    }

    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.namespaces")
    void receive(ConsumerRecord<String, Namespace> record) {
        super.receive(record);
    }


    @Override
    public Collection<Namespace> findAll() {
        return kafkaStore.values();
    }

    @Override
    public Namespace createNamespace(Namespace namespace) {
        return produce(namespace.getName(),namespace);
    }

    @Override
    public Optional<Namespace> findByName(String namespace) {
        return findAll()
                .stream()
                .filter(ns -> securityService.hasRole(ns.getOwner()) && ns.getName().equals(namespace))
                .findFirst();
    }

}
