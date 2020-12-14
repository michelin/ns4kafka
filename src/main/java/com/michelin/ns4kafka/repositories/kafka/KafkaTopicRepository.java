package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ResourceSecurityPolicy;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(
        offsetReset = OffsetReset.EARLIEST,
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaTopicRepository extends KafkaStore<Topic> implements TopicRepository {
    @Inject
    NamespaceRepository namespaceRepository;

    public KafkaTopicRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.topics") String kafkaTopic,
                                      @KafkaClient("topics-producer") Producer<String, Topic> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    @io.micronaut.configuration.kafka.annotation.Topic(value = "${ns4kafka.store.kafka.topics.prefix}.topics")
    void receive(ConsumerRecord<String, Topic> record) {
        super.receive(record);
    }

    @Override
    public List<Topic> findAllForNamespace(String namespace) {
        Optional<Namespace> namespaceOptional = namespaceRepository.findByName(namespace);
        if(namespaceOptional.isPresent()) {
            return kafkaStore.values()
                    .stream()
                    .filter(topic -> namespaceOptional.get()
                            .getPolicies()
                            .stream()
                            .anyMatch(resourceSecurityPolicy -> matchTopicAgainstPolicy(topic, resourceSecurityPolicy))
                    )
                    .collect(Collectors.toList());
        } else {
            return Collections.EMPTY_LIST;
        }
    }
    private boolean matchTopicAgainstPolicy(Topic topic, ResourceSecurityPolicy resourceSecurityPolicy){
        if(resourceSecurityPolicy.getResourceType() == ResourceSecurityPolicy.ResourceType.TOPIC){
            switch (resourceSecurityPolicy.getResourcePatternType()){
                case REGEXP:
                    return topic.getName().matches(resourceSecurityPolicy.getResource());
                case PREFIXED:
                    return topic.getName().startsWith(resourceSecurityPolicy.getResource());
                case LITERAL:
                    return topic.getName().equals(resourceSecurityPolicy.getResource());
            }
        }
        return false;

    }

    @Override
    public Topic create(Topic topic) {
        return this.produce(topic.getCluster()+"/"+topic.getName(), topic);
    }
}
