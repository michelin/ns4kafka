package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import jakarta.inject.Singleton;
import java.util.Collection;
import java.util.Optional;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaAccessControlEntryRepository extends KafkaStore<AccessControlEntry> implements AccessControlEntryRepository {
    public KafkaAccessControlEntryRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.access-control-entries") String kafkaTopic,
                                    @KafkaClient("access-control-entries-producer") Producer<String, AccessControlEntry> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
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

    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.access-control-entries")
    void receive(ConsumerRecord<String, AccessControlEntry> record) {
        super.receive(record);
    }

    @Override
    public Collection<AccessControlEntry> findAll() {
        return getKafkaStore().values();
    }

}
