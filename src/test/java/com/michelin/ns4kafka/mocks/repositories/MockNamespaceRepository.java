package com.michelin.ns4kafka.mocks.repositories;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class MockNamespaceRepository implements NamespaceRepository {
    private final Map<String,Namespace> store = new ConcurrentHashMap<>();
    @Override
    public List<Namespace> findAllForCluster(String cluster) {
        return List.copyOf(store.values());
    }

    @Override
    public Namespace createNamespace(Namespace namespace) {
        store.put(namespace.getName(), namespace);
        return namespace;
    }

    @Override
    public Optional<Namespace> findByName(String namespace) {
        return Optional.ofNullable(store.get(namespace));
    }

    @Override
    public void delete(Namespace namespace) {
        store.remove(namespace.getName());
    }
}
