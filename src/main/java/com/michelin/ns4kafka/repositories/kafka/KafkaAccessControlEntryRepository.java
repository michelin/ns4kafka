package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaAccessControlEntryRepository extends KafkaStore<AccessControlEntry> implements AccessControlEntryRepository {
    public KafkaAccessControlEntryRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.access-control-entries") String kafkaTopic,
                                    @KafkaClient("access-control-entries-producer") Producer<String, AccessControlEntry> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(AccessControlEntry message) {
        //TODO Null validation ? should be done before reaching this ?
        String permission = message.getSpec().getPermission().toString().substring(0,1);
        String patternType = message.getSpec().getResourcePatternType().toString().substring(0,1);

        return String.format("%s/%s:%s:%s:%s",message.getMetadata().getNamespace(), message.getSpec().getResourceType().toString(),permission,patternType,message.getSpec().getResource());
    }

    @Override
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        return this.produce(getMessageKey(accessControlEntry), accessControlEntry);
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
