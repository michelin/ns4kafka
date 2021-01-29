package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED,
        properties = @Property(name = ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, value = "false")
)
public class KafkaTopicRepository extends KafkaStore<Topic> implements TopicRepository {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicRepository.class);
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    public KafkaTopicRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.topics") String kafkaTopic,
                                      @KafkaClient("topics-producer") Producer<String, Topic> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @Override
    String getMessageKey(Topic topic) {
        return topic.getMetadata().getCluster()+"/"+topic.getMetadata().getName();
    }

    @Override
    public Topic create(Topic topic) {
        return this.produce(getMessageKey(topic), topic);
    }

    @io.micronaut.configuration.kafka.annotation.Topic(value = "${ns4kafka.store.kafka.topics.prefix}.topics")
    void receive(ConsumerRecord<String, Topic> record) {
        super.receive(record);
    }

    @Override
    public List<Topic> findAllForNamespace(String namespace) {
        Optional<Namespace> namespaceOptional = namespaceRepository.findByName(namespace);
        List<AccessControlEntry> acls = accessControlEntryRepository.findAllGrantedToNamespace(namespace);
        if(namespaceOptional.isPresent()) {
            return getKafkaStore().values()
                    .stream()
                    .filter(topic -> topic.getMetadata().getCluster().equals(namespaceOptional.get().getCluster()))
                    .filter(topic -> acls.stream()
                            .anyMatch(accessControlEntry -> {
                                //no need to check accessControlEntry.Permission, we want READ, WRITE or OWNER
                                if(accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC){
                                    switch (accessControlEntry.getSpec().getResourcePatternType()){
                                        case PREFIXED:
                                            return topic.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                                        case LITERAL:
                                            return topic.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                                    }
                                }
                                return false;
                            }))
                    .collect(Collectors.toList());
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    @Override
    public List<Topic> findAllForCluster(String cluster) {
        return getKafkaStore().values()
                .stream()
                .filter(topic -> topic.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Topic> findByName(String namespace, String topic) {
        return findAllForNamespace(namespace)
                .stream()
                .filter( t -> t.getMetadata().getName().equals(topic))
                .findFirst();

    }

}
