package com.michelin.ns4kafka.mocks.repositories;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Singleton
public class MockAccessControlEntryRepository implements AccessControlEntryRepository {
    private final Map<String, AccessControlEntry> store = new ConcurrentHashMap<>();
    @Override
    public List<AccessControlEntry> findAllForCluster(String cluster) {
        return null;
    }

    @Override
    public List<AccessControlEntry> findAllGrantedToNamespace(String namespace) {
        return store.values()
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                .collect(Collectors.toList());
    }

    @Override
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        store.put(accessControlEntry.getMetadata().getName(),accessControlEntry);
        return accessControlEntry;
    }

    @Override
    public void deleteByName(String acl) {
        store.remove(acl);
    }

    @Override
    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return Optional.ofNullable(store.get(name));
    }
}
