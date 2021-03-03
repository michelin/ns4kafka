package com.michelin.ns4kafka.mocks.repositories;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaTopicRepository;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import org.apache.zookeeper.Op;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class MockTopicRepository implements TopicRepository {
    private final Map<String, Topic> store = new ConcurrentHashMap<>();
    @Override
    public List<Topic> findAllForNamespace(String namespace) {
        return List.copyOf(store.values());
    }

    @Override
    public List<Topic> findAllForCluster(String cluster) {
        return null;
    }

    @Override
    public Optional<Topic> findByName(String namespace, String topic) {
        return Optional.ofNullable(store.get(topic));
    }

    @Override
    public Topic create(Topic topic) {
        store.put(topic.getMetadata().getName(),topic);
        return topic;
    }

    @Override
    public void delete(Topic topic) {
        store.remove(topic.getMetadata().getName());
    }
}
