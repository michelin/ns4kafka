package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.controllers.TopicController;
import com.michelin.ns4kafka.models.Topic;

import java.util.List;
import java.util.Optional;

public interface TopicRepository {
    List<Topic> findAllForNamespace(String namespace, TopicController.TopicListLimit limit);
    Optional<Topic> findByName(String namespace, String topic);
    Topic create(Topic topic);


}
