package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import io.micronaut.context.annotation.Value;

import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


@Singleton
public class KafkaNamespaceRepository extends KafkaStore<Namespace> implements NamespaceRepository {

    public KafkaNamespaceRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.namespaces") String kafkaTopic) {
        super(kafkaTopic);
    }

    @Override
    String getMessageKey(Namespace namespace) {
        return namespace.getMetadata().getName();
    }

    @Override
    public Namespace createNamespace(Namespace namespace) {
        return produce(getMessageKey(namespace),namespace);
    }

    @Override
    public void delete(Namespace namespace) {
        produce(getMessageKey(namespace),null);
    }

    @Override
    public List<Namespace> findAllForCluster(String cluster) {
        return getKafkaStore().values()
                .stream()
                .filter(namespace -> namespace.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Namespace> findByName(String namespace) {
        return getKafkaStore().values()
                .stream()
                .filter(ns -> ns.getMetadata().getName().equals(namespace))
                .findFirst();
    }

}
