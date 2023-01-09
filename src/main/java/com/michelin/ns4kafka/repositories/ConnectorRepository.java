package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.connector.Connector;

import java.util.List;

public interface ConnectorRepository {
    /**
     * Find all connectors by cluster
     * @param cluster The cluster
     * @return The list of connectors
     */
    List<Connector> findAllForCluster(String cluster);

    /**
     * Create a given connector
     * @param connector The connector to create
     * @return The created connector
     */
    Connector create(Connector connector);

    /**
     * Delete a given connector
     * @param connector The connector to delete
     */
    void delete(Connector connector);
}
