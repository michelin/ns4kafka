package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.repositories.SubjectRepository;
import com.michelin.ns4kafka.services.SubjectService;
import com.michelin.ns4kafka.services.listeners.events.subjects.ApplySubjectEvent;
import com.michelin.ns4kafka.services.listeners.events.subjects.UpdateCompatibilityEvent;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SchemaResponse;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityResponse;
import com.nimbusds.jose.shaded.json.JSONObject;
import com.nimbusds.jose.shaded.json.parser.JSONParser;
import com.nimbusds.jose.shaded.json.parser.ParseException;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.event.ApplicationEventPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * Application event publisher
     */
    @Inject
    public ApplicationEventPublisher applicationEventPublisher;

    /**
     * Run method
     */
    public void run() {
        log.debug("Starting subject collection for cluster {} and schema registry {}", kafkaAsyncExecutorConfig.getName(),
                kafkaAsyncExecutorConfig.getSchemaRegistry().getUrl());

        // List of subjects from schema registry
        List<Subject> subjectsSchemaRegistry = this.collectSubjectsSchemaRegistry();
        // List of subjects from NS4Kafka
        List<Subject> subjectsNS4Kafka = this.subjectRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName());

        List<Subject> toCreate = subjectsNS4Kafka
                .stream()
                .filter(subject -> StringUtils.isBlank(subject.getMetadata().getFinalizer()))
                .filter(subject -> Subject.SubjectPhase.Success.equals(subject.getStatus().getPhase()) && subjectsSchemaRegistry
                    .stream()
                    .noneMatch(subjectSchemaRegistry -> subjectSchemaRegistry.getMetadata().getName().equals(subject.getMetadata().getName())))
                .collect(Collectors.toList());

        log.debug("Found {} subjects to publish", toCreate.size());

        List<Subject> toUpdateCompatibility = subjectsNS4Kafka
                .stream()
                .filter(subject -> subjectsSchemaRegistry
                    .stream()
                    .anyMatch(subjectSchemaRegistry -> {
                        if (subject.getMetadata().getName().equals(subjectSchemaRegistry.getMetadata().getName())) {
                            return !subject.getSpec().getCompatibility().equals(subjectSchemaRegistry.getSpec().getCompatibility());
                        }

                        return false;
                    }))
                .collect(Collectors.toList());

        log.debug("Found {} subjects to update the compatibility", toUpdateCompatibility.size());

        toCreate.forEach(subject -> this.applicationEventPublisher
                .publishEventAsync(new ApplySubjectEvent(subject)));

        toUpdateCompatibility.forEach(subject -> this.applicationEventPublisher
                .publishEventAsync(new UpdateCompatibilityEvent(subject)));

        // TODO: When the schema is different, how to get back to the schema from NS4K
    }

    /**
     * Get all the subjects from the schema registry and build objects
     *
     * @return The subjects of the schema registry
     */
    private List<Subject> collectSubjectsSchemaRegistry() {
        List<String> stringSubjects = this.kafkaSchemaRegistryClient
                .getAllSubjects(KafkaSchemaRegistryClientProxy.PROXY_SECRET, kafkaAsyncExecutorConfig.getName()).body();

        if (stringSubjects == null) {
            return Collections.emptyList();
        }

        return stringSubjects
                .stream()
                .map(stringSubject -> this.buildSubject(stringSubject).get())
                .collect(Collectors.toList());
    }

    /**
     * Build a subject object from the schema registry data
     *
     * @param stringSubject The subject name
     * @return The subject object
     */
    private Optional<Subject> buildSubject(String stringSubject) {
        SchemaResponse schemaResponse = this.kafkaSchemaRegistryClient
                .getSchemaBySubjectAndVersion(KafkaSchemaRegistryClientProxy.PROXY_SECRET, kafkaAsyncExecutorConfig.getName(),
                        stringSubject, "latest").body();

        SubjectCompatibilityResponse compatibility = this.kafkaSchemaRegistryClient
                .getCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET, kafkaAsyncExecutorConfig.getName(),
                        stringSubject).body();

        if (schemaResponse == null || compatibility == null) {
            return Optional.empty();
        }

        return Optional.of(Subject.builder()
                .metadata(ObjectMeta.builder()
                        .name(stringSubject)
                        .build())
                .spec(Subject.SubjectSpec.builder()
                        .schemaContent(Subject.SubjectSpec.Content.builder()
                                .schema(schemaResponse.schema())
                                .build())
                        .compatibility(compatibility.compatibilityLevel())
                        .build())
                .build());
    }

    /**
     * Check if a subject from NS4Kafka is equals to a subject from the schema registry
     *
     * @param subject The subject from NS4Kafka
     * @param subjectSchemaRegistry Ths subject from the schema registry
     * @return true if they are, false otherwise
     */
    private boolean subjectSpecsAreEquals(Subject subject, Subject subjectSchemaRegistry) {
        if (!subject.getSpec().getCompatibility().equals(subjectSchemaRegistry.getSpec().getCompatibility())) {
            return false;
        }

        try {
            JSONParser parser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
            JSONObject schemaNS4Kafka = (JSONObject) parser.parse(subject.getSpec().getSchemaContent().getSchema());
            JSONObject schemaSchemaRegistry = (JSONObject) parser.parse(subjectSchemaRegistry.getSpec().getSchemaContent().getSchema());

            if (!schemaNS4Kafka.equals(schemaSchemaRegistry)) {
                return false;
            }
        } catch (ParseException e) {
            return false;
        }

        return true;
    }
}
