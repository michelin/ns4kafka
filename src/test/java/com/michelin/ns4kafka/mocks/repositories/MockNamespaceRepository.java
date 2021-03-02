package com.michelin.ns4kafka.mocks.repositories;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;

import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Singleton
public class MockNamespaceRepository implements NamespaceRepository {
    @Override
    public List<Namespace> findAllForCluster(String cluster) {
        return null;
    }

    @Override
    public Namespace createNamespace(Namespace namespace) {
        return null;
    }

    @Override
    public Optional<Namespace> findByName(String namespace) {
        return Optional.empty();
    }

    @Override
    public void delete(Namespace namespace) {

    }
}
