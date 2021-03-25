package com.michelin.ns4kafka.services;

import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;

@Singleton
public class TopicService {
    @Inject
    TopicRepository topicRepository;

    public Optional<Topic> findByName(String namespace, String topic) {
        return topicRepository.findByName(namespace, topic);
    }

    public List<Topic> findAllForNamespace(String namespace) {
        return topicRepository.findAllForNamespace(namespace);
    }
}
