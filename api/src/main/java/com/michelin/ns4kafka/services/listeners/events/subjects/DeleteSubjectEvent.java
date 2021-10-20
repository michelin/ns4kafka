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
 * Event for deleting a subject to the schema registry
 */
@Slf4j
public class DeleteSubjectEvent extends SubjectEvent {
    /**
     * Constructor
     *
     * @param subject The subject of the event
     */
    public DeleteSubjectEvent(Subject subject) {
        super(subject);
    }

    /**
     * For an delete subject event, delete it from the schema registry
     */
    @Override
    public void process() {
        try {
            this.kafkaSchemaRegistryClient.deleteBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    resource.getMetadata().getCluster(), resource.getMetadata().getName(), false);

            this.kafkaSchemaRegistryClient.deleteBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                    resource.getMetadata().getCluster(), resource.getMetadata().getName(), true);

            this.subjectRepository.delete(resource);
        }  catch (Exception e) {
            log.error("Error deleting subject [{}] on schema registry's cluster [{}]",
                    resource.getMetadata().getName(), resource.getMetadata().getCluster(), e);

            resource.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            resource.getMetadata().setFinalizer("to_delete");

            this.subjectService.delete(resource);
        }
    }
}
