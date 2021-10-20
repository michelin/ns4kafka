package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Subject;
import com.michelin.ns4kafka.repositories.SubjectRepository;
import com.michelin.ns4kafka.services.listeners.events.subjects.ApplySubjectEvent;
import com.michelin.ns4kafka.services.listeners.events.subjects.DeleteSubjectEvent;
import com.michelin.ns4kafka.services.schema.registry.KafkaSchemaRegistryClientProxy;
import com.michelin.ns4kafka.services.schema.registry.client.KafkaSchemaRegistryClient;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityRequest;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityCheckResponse;
import com.michelin.ns4kafka.services.schema.registry.client.entities.SubjectCompatibilityResponse;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class SubjectService {
    /**
     * ACLs service
     */
    @Inject
    AccessControlEntryService accessControlEntryService;

    /**
     * Subject repository
     */
    @Inject
    SubjectRepository subjectRepository;

    /**
     * Schema Registry client
     */
    @Inject
    KafkaSchemaRegistryClient kafkaSchemaRegistryClient;

    /**
     * Application event publisher
     */
    @Inject
    public ApplicationEventPublisher applicationEventPublisher;

    /**
     * Publish a subject to the subjects technical topic
     *
     * @param subject The subject to publish
     * @return The created subject
     */
    public Subject create(Subject subject) {
        Subject created = this.subjectRepository.create(subject);

        this.applicationEventPublisher.publishEventAsync(new ApplySubjectEvent(subject));

        return created;
    }

    /**
     * Delete a subject from the subjects technical topic
     *
     * @param subject The subject to delete
     */
    public void delete(Subject subject) {
        this.subjectRepository.delete(subject);

        this.applicationEventPublisher.publishEventAsync(new DeleteSubjectEvent(subject));
    }

    /**
     * Find a subject by name
     *
     * @param namespace The namespace
     * @param name The name of the subject
     * @return A subject matching the given name
     */
    public Optional<Subject> findByName(Namespace namespace, String name) {
        return this.findAllForNamespace(namespace)
                .stream()
                .filter(subject -> subject.getMetadata().getName().equals(name))
                .findFirst();
    }

    /**
     * Find all subjects on a given namespace
     *
     * @param namespace The namespace used to research the subjects
     * @return A list of subjects
     */
    public List<Subject> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = this.accessControlEntryService.findAllGrantedToNamespace(namespace);

        return this.subjectRepository.findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(subject -> acls.stream().anyMatch(accessControlEntry -> {
                    // need to check accessControlEntry.Permission, we want OWNER
                    if (accessControlEntry.getSpec().getPermission() != AccessControlEntry.Permission.OWNER) {
                        return false;
                    }

                    if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.SUBJECT) {
                        switch (accessControlEntry.getSpec().getResourcePatternType()) {
                            case PREFIXED:
                                return subject.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                            case LITERAL:
                                return subject.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                        }
                    }

                    return false;
                }))
                .collect(Collectors.toList());
    }

    /**
     * Get the current compatibility mode of the given subject
     *
     * @param cluster The cluster linked with the Schema Registry to call
     * @param subject The subject to update the compatibility
     * @return The response of the schema registry
     */
    public HttpResponse<SubjectCompatibilityResponse> getCurrentCompatibilityBySubject(String cluster, Subject subject) {
        return this.kafkaSchemaRegistryClient.getCurrentCompatibilityBySubject(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, subject.getMetadata().getName(), true);
    }

    /**
     * Validate the subject compatibility against the Schema Registry
     *
     * @param cluster The cluster linked with the Schema Registry to call
     * @param subject The subject to update the compatibility
     * @param subjectCompatibilityRequest The compatibility to apply
     */
    public void updateSubjectCompatibility(String cluster, Subject subject, SubjectCompatibilityRequest subjectCompatibilityRequest) {
        this.kafkaSchemaRegistryClient.updateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, subject.getMetadata().getName(), subjectCompatibilityRequest);
    }

    /**
     * Validate the subject compatibility against the Schema Registry
     *
     * @param cluster The cluster linked with the Schema Registry to call
     * @param subject The subject to validate
     * @return A list of errors
     */
    public List<String> validateSubjectCompatibility(String cluster, Subject subject) {
        HttpResponse<SubjectCompatibilityCheckResponse> response = this.kafkaSchemaRegistryClient.validateSubjectCompatibility(KafkaSchemaRegistryClientProxy.PROXY_SECRET,
                cluster, subject.getMetadata().getName(), subject.getSpec().getSchemaContent());

        if (response.getBody().isPresent() && !response.getBody().get().isCompatible()) {
            return List.of("The schema registry rejected the given subject for compatibility reason");
        }

        return Collections.emptyList();
    }

    /**
     * Does the namespace is owner of the given subject
     *
     * @param namespace The namespace
     * @param subjectName The name of the subject
     * @return true if it's owner, false otherwise
     */
    public boolean isNamespaceOwnerOfSubject(Namespace namespace, String subjectName) {
        return this.accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(), AccessControlEntry.ResourceType.SUBJECT,
                subjectName);
    }
}
