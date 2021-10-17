package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.context.annotation.Value;

import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class KafkaTopicRepository extends KafkaStore<Topic> implements TopicRepository {

    public KafkaTopicRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.topics") String kafkaTopic) {
        super(kafkaTopic);
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

    @Override
    public List<Topic> findAllForCluster(String cluster) {
        return getKafkaStore().values()
                .stream()
                .filter(topic -> topic.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

}
