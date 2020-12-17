package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.controllers.TopicController;
import com.michelin.ns4kafka.models.Topic;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TopicRepository {
    List<Topic> findAllForNamespace(String namespace, TopicController.TopicListLimit limit);

    /***
     *
     * @param cluster the cluster id
     * @return the list of all topics for this cluster as a KV Map with :<br>
     * key : String : Topic Name<br>
     * value : Topic : Topic data<br>
     */
    List<Topic> findAllForCluster(String cluster);
    Optional<Topic> findByName(String namespace, String topic);
    Topic create(Topic topic);


}
