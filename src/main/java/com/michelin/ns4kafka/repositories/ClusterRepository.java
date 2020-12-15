package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.KafkaCluster;
import com.michelin.ns4kafka.models.Namespace;

import java.util.Collection;
import java.util.Optional;

public interface ClusterRepository {
    Collection<KafkaCluster> findAll();
    Optional<KafkaCluster> findByName(String cluster);
    KafkaCluster create(KafkaCluster cluster);
}
