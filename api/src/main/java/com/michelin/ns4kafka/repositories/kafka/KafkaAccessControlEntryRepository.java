package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
        return  accessControlEntry.getMetadata().getNamespace() + "-" + sha256_last8(accessControlEntry.getSpec().toString());
    }

    @Override
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        //TODO name must be set before this layer ?
        String messageKey = getMessageKey(accessControlEntry);
        accessControlEntry.getMetadata().setName(messageKey);
        return this.produce(messageKey, accessControlEntry);
    }

    @Override
    public void deleteByName(String acl) {
        if(getKafkaStore().containsKey(acl)) {
            AccessControlEntry toDelete = getKafkaStore().get(acl);
            produce(acl,null);
        }else{
            throw new KafkaStoreException("Acl "+ acl+" does not exists");
        }

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
    public List<AccessControlEntry> findAllForCluster(String cluster) {
        return getKafkaStore().values()
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

    @Override
    public List<AccessControlEntry> findAllGrantedToNamespace(String namespace) {
        return getKafkaStore().values()
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                .collect(Collectors.toList());
    }

}
