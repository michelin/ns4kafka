package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.Value;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Singleton;
import javax.xml.crypto.dsig.DigestMethod;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    String getMessageKey(AccessControlEntry message) {

        //(grantor) namespace + AccessControlEntrySpec
        String sha_input = String.format("%s:%s:%s:%s:%s",
                message.getSpec().getGrantedTo(),
                message.getSpec().getResourceType().toString(),
                message.getSpec().getPermission().toString(),
                message.getSpec().getResourcePatternType().toString(),
                message.getSpec().getResource());

        return  message.getMetadata().getNamespace()+"-" + sha256_last8(sha_input);
    }

    @Override
    public AccessControlEntry create(AccessControlEntry accessControlEntry) {
        String messageKey = getMessageKey(accessControlEntry);
        accessControlEntry.getMetadata().setName(messageKey);
        return this.produce(messageKey, accessControlEntry);
    }

    @Override
    public List<AccessControlEntry> deleteByName(String acl) {
        if(getKafkaStore().containsKey(acl)) {
            AccessControlEntry toDelete = getKafkaStore().get(acl);
            produce(acl,null);
            return findAllGrantedToNamespace(toDelete.getMetadata().getNamespace());
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

    private String sha256_last8(String originalString) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest(
                    originalString.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder(2 * encodedhash.length);
            for (int i = 0; i < encodedhash.length; i++) {
                String hex = Integer.toHexString(0xff & encodedhash[i]);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString().substring(56);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //should anything happen, we still need a unique K (might not be urlencode compliant but goodenough)
        return originalString;


    }
}
