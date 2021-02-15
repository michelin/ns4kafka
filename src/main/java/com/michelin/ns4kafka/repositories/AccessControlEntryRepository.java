package com.michelin.ns4kafka.repositories;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;

import java.util.List;
import java.util.Optional;

public interface AccessControlEntryRepository {
    List<AccessControlEntry> findAllForCluster(String cluster);
    List<AccessControlEntry> findAllGrantedToNamespace(String namespace);

    AccessControlEntry create(AccessControlEntry accessControlEntry);
    List<AccessControlEntry> deleteByName(String acl);
    Optional<AccessControlEntry> findByName(String namespace, String name);

}
