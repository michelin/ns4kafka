package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import io.micronaut.context.annotation.Value;

import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class KafkaConnectorRepository extends KafkaStore<Connector> implements ConnectorRepository {
    public KafkaConnectorRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.connectors") String kafkaTopic) {
        super(kafkaTopic);
    }

    @Override
    String getMessageKey(Connector roleBinding) {
        return roleBinding.getMetadata().getNamespace() + "/" + roleBinding.getMetadata().getName();
    }
    @Override
    public Connector create(Connector connector) {
        return this.produce(getMessageKey(connector),connector);
    }

    @Override
    public void delete(Connector connector) {
        this.produce(getMessageKey(connector),null);
    }

    @Override
    public List<Connector> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(connector -> connector.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

}
