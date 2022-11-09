package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.Topic;

import java.util.List;

public interface TopicRepository {
    /**
     * Find all topics by cluster
     * @param cluster The cluster
     * @return The list of topics
     */
    List<Topic> findAllForCluster(String cluster);

    /**
     * Create a given topic
     * @param topic The topic to create
     * @return The created topic
     */
    Topic create(Topic topic);

    /**
     * Delete a given topic
     * @param topic The topic to delete
     */
    void delete(Topic topic);
}
