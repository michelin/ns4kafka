package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.Topic;

import java.util.List;

public interface TopicRepository {
    List<Topic> findAllForNamespace(String namespace);
    Topic create(Topic topic);


}
