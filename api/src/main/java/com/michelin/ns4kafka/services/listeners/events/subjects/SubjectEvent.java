package com.michelin.ns4kafka.services.listeners.events.subjects;

import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.repositories.SubjectRepository;
import com.michelin.ns4kafka.services.SubjectService;
import com.michelin.ns4kafka.services.listeners.events.AbstractResourceEvent;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import io.micronaut.context.ApplicationContext;

public abstract class SubjectEvent extends AbstractResourceEvent<Subject> {
    /**
     * The kafka schema registry client
     */
    KafkaSchemaRegistryClient kafkaSchemaRegistryClient;

    /**
     * Subject service
     */
    SubjectService subjectService;

    /**
     * Subject repository
     */
    SubjectRepository subjectRepository;

    /**
     * Constructor
     *
     * @param subject The subject object
     */
    protected SubjectEvent(Subject subject) {
        super(subject);
    }

    /**
     * Inject the services required to process a Subject event
     *
     * @param applicationContext The application context
     */
    @Override
    public void injectServices(ApplicationContext applicationContext) {
        this.kafkaSchemaRegistryClient = applicationContext.getBean(KafkaSchemaRegistryClient.class);
        this.subjectService = applicationContext.getBean(SubjectService.class);
        this.subjectRepository = applicationContext.getBean(SubjectRepository.class);
    }
}
