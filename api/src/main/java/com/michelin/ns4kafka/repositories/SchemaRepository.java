package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Schema;

import java.util.List;

public interface SchemaRepository {
    Schema create(Schema schema);
    List<Schema> findAllForCluster(String cluster);
}
