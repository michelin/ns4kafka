package com.michelin.ns4kafka.repository;

import com.michelin.ns4kafka.model.Namespace;
import java.util.List;
import java.util.Optional;

/**
 * Namespace repository.
 */
public interface NamespaceRepository {
    List<Namespace> findAllForCluster(String cluster);

    Namespace createNamespace(Namespace namespace);

    Optional<Namespace> findByName(String namespace);

    void delete(Namespace namespace);
}
