package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import io.micronaut.context.annotation.Value;

import javax.inject.Singleton;
import java.util.Collection;
import java.util.Optional;

@Singleton
public class KafkaAccessControlEntryRepository extends KafkaStore<AccessControlEntry> implements AccessControlEntryRepository {
    public KafkaAccessControlEntryRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.access-control-entries") String kafkaTopic) {
        super(kafkaTopic);
    }

    @Override
    String getMessageKey(AccessControlEntry accessControlEntry) {
        return  accessControlEntry.getMetadata().getNamespace() + "/" + accessControlEntry.getMetadata().getName();
    }

    @Override
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        return this.produce(getMessageKey(accessControlEntry), accessControlEntry);
    }

    @Override
    public void delete(AccessControlEntry accessControlEntry) {
        produce(getMessageKey(accessControlEntry),null);
    }

    @Override
    public Optional<AccessControlEntry> findByName(String namespace, String name) {
        return getKafkaStore().values()
                .stream()
                .filter(ace -> ace.getMetadata().getNamespace().equals(namespace))
                .filter(ace -> ace.getMetadata().getName().equals(name))
                .findFirst();
    }

    @Override
    public Collection<AccessControlEntry> findAll() {
        return getKafkaStore().values();
    }

}
