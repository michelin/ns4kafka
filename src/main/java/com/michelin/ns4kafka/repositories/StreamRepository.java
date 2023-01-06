package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.KafkaStream;

import java.util.List;

public interface StreamRepository {
    /***
     *
     * @param cluster the cluster id
     * @return the list of all kafkastreams for this cluster as a KV Map with :<br>
     * key : String : KafkaStream Name<br>
     * value : KafkaStream : KafkaStream data<br>
     */
    List<KafkaStream> findAllForCluster(String cluster);

    KafkaStream create(KafkaStream stream);

    void delete(KafkaStream stream);
}
