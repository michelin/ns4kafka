package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.ConnectCluster;

import java.util.List;

public interface ConnectClusterRepository {
    List<ConnectCluster> findAll();
    List<ConnectCluster> findAllForCluster(String cluster);
    ConnectCluster create(ConnectCluster connectCluster);
    void delete(ConnectCluster connectCluster);
}
