package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.KafkaStream;
import java.util.List;

/**
 * Stream repository.
 */
public interface StreamRepository {
    List<KafkaStream> findAllForCluster(String cluster);

    KafkaStream create(KafkaStream stream);

    void delete(KafkaStream stream);
}
