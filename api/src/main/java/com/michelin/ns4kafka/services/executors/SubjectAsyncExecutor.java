package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.repositories.SubjectRepository;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import io.micronaut.context.annotation.EachBean;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class SubjectAsyncExecutor {
    /**
     * Configuration
     */
    @Inject
    KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    /**
     * Schema repository
     */
    @Inject
    SubjectRepository subjectRepository;

    /**
     * Kafka schema registry client
     */
    @Inject
    KafkaSchemaRegistryClient kafkaSchemaRegistryClient;

    /**
     * Run method
     */
    public void run() {
        log.debug("Starting subject collection for cluster {} and schema registry {}", kafkaAsyncExecutorConfig.getName(),
                kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl());

        List<Subject> subjectsToPublishes = this.subjectRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(subject -> Arrays.asList(Subject.SubjectPhase.Pending, Subject.SubjectPhase.Failed).contains(subject.getStatus().getPhase()))
                .collect(Collectors.toList());

        log.debug("{} schemas found to publish ({} pending, {} failed)", subjectsToPublishes.size(),
                subjectsToPublishes.stream().filter(schema -> schema.getStatus().getPhase().equals(Subject.SubjectPhase.Pending)).count(),
                subjectsToPublishes.stream().filter(schema -> schema.getStatus().getPhase().equals(Subject.SubjectPhase.Failed)).count());

        subjectsToPublishes.forEach(this::publishSubject);

        List<Subject> subjectsToDelete = this.subjectRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(schema -> "to_delete".equals(schema.getMetadata().getFinalizer()))
                .collect(Collectors.toList());

        log.debug("{} schemas found to delete", subjectsToDelete.size());

        subjectsToDelete.forEach(this::deleteSubject);
    }

    /**
     * Publish given subject to the schema registry of the current managed cluster
     *
     * @param subject The subject to publish
     */
    private void publishSubject(Subject subject) {
        try {
            this.kafkaSchemaRegistryClient.publish(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    subject.getMetadata().getCluster(), subject.getMetadata().getName(),
                    subject.getSpec().getSchemaContent());

            log.info("Success deploying schema [{}] on schema registry [{}] of kafka cluster [{}]",
                    subject.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName());

            subject.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            subject.setStatus(Subject.SubjectStatus.ofSuccess("Schema published"));

            this.subjectRepository.create(subject);
        } catch (Exception e) {
            log.error("Error deploying subject [{}] on schema registry [{}] of kafka cluster [{}]",
                    subject.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName(), e);

            subject.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            subject.setStatus(Subject.SubjectStatus.ofFailed("Subject failed"));

            this.subjectRepository.create(subject);
        }
    }

    /**
     * Delete given subject from the schema registry of the current managed cluster
     *
     * @param subject The subject to delete
     */
    private void deleteSubject(Subject subject) {
        try {
            this.kafkaSchemaRegistryClient.deleteBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    subject.getMetadata().getCluster(), subject.getMetadata().getName(),
                    false);

            this.kafkaSchemaRegistryClient.deleteBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    subject.getMetadata().getCluster(), subject.getMetadata().getName(),
                    true);

            log.info("Success deleting subject [{}] from schema registry [{}] of kafka cluster [{}]",
                    subject.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName());

            this.subjectRepository.delete(subject);
        } catch (Exception e) {
            log.error("Error deleting subject [{}] on schema registry [{}] of kafka cluster [{}]",
                    subject.getMetadata().getName(),
                    this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(),
                    this.kafkaAsyncExecutorConfig.getName(), e);

            subject.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            subject.getMetadata().setFinalizer("to_delete");

            this.subjectRepository.create(subject);
        }
    }
}
