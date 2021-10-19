package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.repositories.SubjectRepository;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import io.micronaut.context.annotation.EachBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

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

        this.collectSubjectsToPublish();
        this.collectSubjectsToDelete();
    }

    /**
     * Get all the subjects to publish
     *
     * Subjects in pending/failed state, subjects that are success but not in the
     * schema registry anymore are flagged to be published again
     */
    private void collectSubjectsToPublish() {
        List<String> subjectsFromSchemaRegistry = this.getAllSubjectsFromSchemaRegistry();

        List<Subject> subjectsToPublish = this.subjectRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                .stream()
                .filter(subject -> StringUtils.isBlank(subject.getMetadata().getFinalizer()))
                .filter(subject ->
                        (Arrays.asList(Subject.SubjectPhase.Pending, Subject.SubjectPhase.Failed).contains(subject.getStatus().getPhase())) ||
                        (Subject.SubjectPhase.Success.equals(subject.getStatus().getPhase()) && !subjectsFromSchemaRegistry.contains(subject.getMetadata().getName())))
                .collect(Collectors.toList());

        log.debug("Found {} subjects to publish", subjectsToPublish.size());

        subjectsToPublish.forEach(this::publishSubject);
    }

    /**
     * Get all the subjects to delete
     *
     * Subjects with a finalizer to "to_delete", subjects that are not in NS4Kafka
     * are flagged to be deleted
     */
    private void collectSubjectsToDelete() {
        List<Subject> subjectsFromNS4Kafka = this.subjectRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName());

        List<Subject> subjectsToDelete = subjectsFromNS4Kafka
                .stream()
                .filter(schema -> "to_delete".equals(schema.getMetadata().getFinalizer()))
                .collect(Collectors.toList());

        log.debug("Found {} subjects marked as to delete", subjectsToDelete.size());

        subjectsToDelete.forEach(this::deleteSubject);

        List<String> subjectsNotInNs4Kafka = this.getAllSubjectsFromSchemaRegistry()
                .stream()
                .filter(subjectFromSchemaRegistry -> !subjectsFromNS4Kafka
                        .stream()
                        .map(subject -> subject.getMetadata().getName())
                        .collect(Collectors.toList())
                        .contains(subjectFromSchemaRegistry))
                .collect(Collectors.toList());

        log.debug("Found {} subjects in the schema registry missing in NS4Kafka", subjectsNotInNs4Kafka.size());

        subjectsNotInNs4Kafka.forEach(subjectFromNS4Kafka -> this.deleteSubjectFromSchemaRegistry(this.kafkaAsyncExecutorConfig.getName(),
                subjectFromNS4Kafka));
    }

    /**
     * Get all the subjects currently in the schema registry
     *
     * @return The list of subjects in the schema registry
     */
    private List<String> getAllSubjectsFromSchemaRegistry() {
        return this.kafkaSchemaRegistryClient.getAllSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                kafkaAsyncExecutorConfig.getName()).body();
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
            this.deleteSubjectFromSchemaRegistry(this.kafkaAsyncExecutorConfig.getName(), subject.getMetadata().getName());

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

    /**
     * Delete the subject from its schema registry
     *
     * @param cluster The cluster linked with the schema registry of the subject
     * @param subject The subject to delete
     */
    private void deleteSubjectFromSchemaRegistry(String cluster, String subject) {
        this.kafkaSchemaRegistryClient.deleteBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, subject, false);

        this.kafkaSchemaRegistryClient.deleteBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, subject, true);

        log.info("Success deleting subject [{}] from schema registry [{}] of kafka cluster [{}]",
                subject, this.kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl(), this.kafkaAsyncExecutorConfig.getName());

    }
}
