package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.ConnectCluster;
import com.michelin.ns4kafka.repositories.ConnectClusterRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaConnectClusterRepository extends KafkaStore<ConnectCluster> implements ConnectClusterRepository {
    public KafkaConnectClusterRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.connect-workers") String kafkaTopic,
                                         @KafkaClient("connect-workers") Producer<String, ConnectCluster> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    public List<ConnectCluster> findAll() {
        return new ArrayList<>(getKafkaStore().values());
    }

    @Override
    public List<ConnectCluster> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(connectCluster -> connectCluster.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

    @Override
    public ConnectCluster create(ConnectCluster connectCluster) {
        return this.produce(getMessageKey(connectCluster), connectCluster);
    }

    @Override
    public void delete(ConnectCluster connectCluster) {
        this.produce(getMessageKey(connectCluster),null);
    }

    @Override
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.connect-workers")
    void receive(ConsumerRecord<String, ConnectCluster> record) {
        super.receive(record);
    }

    @Override
    String getMessageKey(ConnectCluster connectCluster) {
        return connectCluster.getMetadata().getNamespace() + "/" + connectCluster.getMetadata().getName();
    }
}
