package com.michelin.ns4kafka.services.listeners.events.subjects;

import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityRequest;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Date;

/**
 * Event for update the compatibility of a subject to the schema registry
 */
@Slf4j
public class UpdateCompatibilityEvent extends SubjectEvent {
    /**
     * Constructor
     *
     * @param subject The subject of the event
     */
    public UpdateCompatibilityEvent(Subject subject) {
        super(subject);
    }

    /**
     * For an update compatibility event, update the compatibility of the subject to the schema registry
     */
    @Override
    public void process() {
        this.kafkaSchemaRegistryClient.updateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                resource.getMetadata().getCluster(), resource.getMetadata().getName(),
                SubjectCompatibilityRequest.builder()
                        .compatibility(resource.getSpec().getCompatibility())
                        .build());

        resource.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        resource.setStatus(Subject.SubjectStatus.ofSuccess("Schema published"));

        this.subjectRepository.create(resource);
    }
}
