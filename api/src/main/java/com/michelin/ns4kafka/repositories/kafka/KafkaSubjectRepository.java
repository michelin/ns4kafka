package com.michelin.ns4kafka.repositories.kafka;

import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.repositories.SubjectRepository;
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
        groupId = "${ns4kafka.store.kafka.group-id}",
        offsetStrategy = OffsetStrategy.DISABLED
)
public class KafkaSubjectRepository extends KafkaStore<Subject> implements SubjectRepository {
    /**
     * Constructor
     *
     * @param kafkaTopic The technical topic for schemas
     * @param kafkaProducer The kafka producer for schemas
     */
    public KafkaSubjectRepository(@Value("${ns4kafka.store.kafka.topics.prefix}.schemas") String kafkaTopic,
                                  @KafkaClient("schemas-producer") Producer<String, Subject> kafkaProducer) {
        super(kafkaTopic, kafkaProducer);
    }

    /**
     * Produce a new schema message to the schemas technical topic
     *
     * @param subject The schema to register
     * @return The registered schema
     */
    @Override
    public Subject create(Subject subject) {
        return this.produce(getMessageKey(subject), subject);
    }

    /**
     * Consume records from the schemas technical topic
     *
     * @param record The record
     */
    @Topic(value = "${ns4kafka.store.kafka.topics.prefix}.schemas")
    void receive(ConsumerRecord<String, Subject> record) {
        super.receive(record);
    }

    /**
     * "Delete" a schema from the schemas technical topic by pushing a new null message into it
     *
     * @param subject The schema to delete, used to compute the key
     */
    @Override
    public void delete(Subject subject) {
        this.produce(getMessageKey(subject),null);
    }

    /**
     * Find all schemas for cluster
     *
     * @param cluster The cluster
     * @return A list of schemas
     */
    @Override
    public List<Subject> findAllForCluster(String cluster) {
        return getKafkaStore().values().stream()
                .filter(schema -> schema.getMetadata().getCluster().equals(cluster))
                .collect(Collectors.toList());
    }

    /**
     * Build a key according to a given schema resource
     *
     * @param subject The schema resource
     * @return The key
     */
    @Override
    String getMessageKey(Subject subject) {
        return subject.getMetadata().getNamespace() + "/" + subject.getMetadata().getName();
    }
}
