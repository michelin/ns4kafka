package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.AccessControlEntry;

import java.util.Collection;
import java.util.Optional;

public interface AccessControlEntryRepository {
    Collection<AccessControlEntry> findAll();
    Optional<AccessControlEntry> findByName(String namespace, String name);
    AccessControlEntry create(AccessControlEntry accessControlEntry);
    void delete(AccessControlEntry accessControlEntry);


}
