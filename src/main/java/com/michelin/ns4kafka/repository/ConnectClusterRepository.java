package com.michelin.ns4kafka.repository;

import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import java.util.List;

/**
 * Repository to manage Kafka Connect clusters.
 */
public interface ConnectClusterRepository {
    List<ConnectCluster> findAll();

    List<ConnectCluster> findAllForCluster(String cluster);

    ConnectCluster create(ConnectCluster connectCluster);

    void delete(ConnectCluster connectCluster);
}
