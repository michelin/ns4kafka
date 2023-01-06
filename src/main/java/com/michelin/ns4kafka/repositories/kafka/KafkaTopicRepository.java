package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
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
public class KafkaTopicRepository extends KafkaStore<Topic> implements TopicRepository {

    public KafkaTopicRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.topics") String kafkaTopic,
                                      @KafkaClient("topics-producer") Producer<String, Topic> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(Topic topic) {
        return topic.getMetadata().getCluster()+"/"+topic.getMetadata().getName();
    }

    /**
     * Create a given topic
     * @param topic The topic to create
     * @return The created topic
     */
    @Override
    public Topic create(Topic topic) {
        return this.produce(getMessageKey(topic), topic);
    }

    /**
     * Delete a given topic
     * @param topic The topic to delete
     */
    @Override
    public void delete(Topic topic) {
        this.produce(getMessageKey(topic),null);
    }

    @io.micronaut.configuration.kafka.annotation.Topic(value = "${ns4kafka.store.kafka.topics.prefix}.topics")
    void receive(ConsumerRecord<String, Topic> record) {
        super.receive(record);
    }

    /**
     * Find all topics
     * @return The list of topics
     */
    @Override
    public List<Topic> findAll() {
        return new ArrayList<>(getKafkaStore().values());
    }

    /**
     * Find all topics by cluster
     * @param cluster The cluster
     * @return The list of topics
     */
    @Override
    public List<Topic> findAllForCluster(String cluster) {
        return getKafkaStore().values()
                .stream()
                .filter(topic -> topic.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }
}
