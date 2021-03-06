package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.Topic;

import java.util.List;

public interface TopicRepository {
    //List<Topic> findAllForNamespace(Namespace namespace);

    /***
     *
     * @param cluster the cluster id
     * @return the list of all topics for this cluster as a KV Map with :<br>
     * key : String : Topic Name<br>
     * value : Topic : Topic data<br>
     */
    List<Topic> findAllForCluster(String cluster);

    Topic create(Topic topic);

    void delete(Topic topic);

}
