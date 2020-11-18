package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.Namespace;

import java.util.Collection;
import java.util.Optional;

public interface NamespaceRepository {
    Collection<Namespace> findAll();
    Namespace createNamespace(Namespace namespace);
    Optional<Namespace> findByName(String namespace);


}
