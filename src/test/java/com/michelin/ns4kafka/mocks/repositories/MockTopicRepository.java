package com.michelin.ns4kafka.mocks.repositories;

import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaTopicRepository;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import org.apache.zookeeper.Op;

import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Singleton
public class MockTopicRepository implements TopicRepository {
    @Override
    public List<Topic> findAllForNamespace(String namespace) {
        return null;
    }

    @Override
    public List<Topic> findAllForCluster(String cluster) {
        return null;
    }

    @Override
    public Optional<Topic> findByName(String namespace, String topic) {
        if("test".equals(namespace) && "toto".equals(topic)){
            return Optional.of(Topic.builder().metadata(ObjectMeta.builder().name("toto").build()).build());
        }
        return Optional.empty();
    }

    @Override
    public Topic create(Topic topic) {
        return null;
    }

    @Override
    public void delete(Topic topic) {

    }
}
