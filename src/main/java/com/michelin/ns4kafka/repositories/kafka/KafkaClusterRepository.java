package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.KafkaCluster;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ResourceSecurityPolicy;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.ClusterRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED,
        properties = @Property(name = ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, value = "false")
)
public class KafkaClusterRepository extends KafkaStore<KafkaCluster> implements ClusterRepository {

    public KafkaClusterRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.clusters") String kafkaTopic,
                                  @KafkaClient("clusters-producer") Producer<String, KafkaCluster> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @io.micronaut.configuration.kafka.annotation.Topic(value = "${ns4kafka.store.kafka.topics.prefix}.clusters")
    void receive(ConsumerRecord<String, KafkaCluster> record) {
        super.receive(record);
    }


    @Override
    public Collection<KafkaCluster> findAll() {
        return kafkaStore.values();
    }

    @Override
    public Optional<KafkaCluster> findByName(String name) {
        return findAll()
                .stream()
                .filter(cluster -> cluster.getName().equals(name))
                .findFirst();
    }

    @Override
    public KafkaCluster create(KafkaCluster cluster) {
        return this.produce(cluster.getName(),cluster);
    }
}
