package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.Connector;

import java.util.List;

public interface ConnectorRepository {
    List<Connector> findAllForCluster(String cluster);

    Connector create(Connector topic);

    void delete(Connector topic);
}
