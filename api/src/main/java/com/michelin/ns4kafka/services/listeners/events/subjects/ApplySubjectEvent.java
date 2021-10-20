package com.michelin.ns4kafka.services.listeners.events.subjects;

import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.repositories.SubjectRepository;
import com.michelin.ns4kafka.services.SubjectService;
import com.michelin.ns4kafka.services.listeners.events.AbstractResourceEvent;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import io.micronaut.context.ApplicationContext;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Date;

/**
 * Event for publishing a subject to the schema registry
 */
@Slf4j
public class ApplySubjectEvent extends SubjectEvent {
    /**
     * Constructor
     *
     * @param subject The subject of the event
     */
    public ApplySubjectEvent(Subject subject) {
        super(subject);
    }

    /**
     * For an apply subject event, publish it to the schema registry
     */
    @Override
    public void process() {
        try {
            this.kafkaSchemaRegistryClient.publish(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    resource.getMetadata().getCluster(), resource.getMetadata().getName(),
                    resource.getSpec().getSchemaContent());

            resource.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            resource.setStatus(Subject.SubjectStatus.ofSuccess("Schema published"));

            // Marks the subject as successful in NS4Kafka
            this.subjectRepository.create(resource);
        } catch (Exception e) {
            log.error("Error deploying subject [{}] on schema registry's cluster [{}]",
                    resource.getMetadata().getName(), resource.getMetadata().getCluster(), e);

            resource.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            resource.setStatus(Subject.SubjectStatus.ofFailed("Subject failed"));

            // Publish a new event
            this.subjectService.create(resource);
        }
    }
}
