package com.michelin.ns4kafka.mocks.repositories;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;

import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Singleton
public class MockAccessControlEntryRepository implements AccessControlEntryRepository {
    @Override
    public List<AccessControlEntry> findAllForCluster(String cluster) {
        return null;
    }

    @Override
    public List<AccessControlEntry> findAllGrantedToNamespace(String namespace) {
        return null;
    }

    @Override
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        return null;
    }

    @Override
    public List<AccessControlEntry> deleteByName(String acl) {
        return null;
    }

    @Override
    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return Optional.empty();
    }
}
