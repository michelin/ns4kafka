package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaTopicRepository extends KafkaStore<Topic> implements TopicRepository {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicRepository.class);

    public KafkaTopicRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.topics") String kafkaTopic,
                                      @KafkaClient("topics-producer") Producer<String, Topic> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(Topic topic) {
        return topic.getMetadata().getCluster()+"/"+topic.getMetadata().getName();
    }

    @Override
    public Topic create(Topic topic) {
        return this.produce(getMessageKey(topic), topic);
    }

    @Override
    public void delete(Topic topic) {
        this.produce(getMessageKey(topic),null);
    }

    @io.micronaut.configuration.kafka.annotation.Topic(value = "${ns4kafka.store.kafka.topics.prefix}.topics")
    void receive(ConsumerRecord<String, Topic> record) {
        super.receive(record);
    }

    //@Override
    //public List<Topic> findAllForNamespace(Namespace namespace) {
    //        return getKafkaStore().values()
    //                .stream()
    //                .filter(topic -> topic.getMetadata().getCluster().equals(namespace.getCluster()))
    //                .collect(Collectors.toList());
    //}

    @Override
    public List<Topic> findAllForCluster(String cluster) {
        return getKafkaStore().values()
                .stream()
                .filter(topic -> topic.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

}
