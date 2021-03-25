package com.michelin.ns4kafka.controllers;

import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.validation.Valid;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.services.KafkaAsyncExecutor;
import com.michelin.ns4kafka.services.TopicService;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Topics")
@Controller(value = "/api/namespaces/{namespace}/topics")
public class TopicController {
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    TopicService topicService;
    @Inject
    TopicRepository topicRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;
    @Inject
    ApplicationContext applicationContext;

    /**
     * @param namespace The namespace to query
     * @return The list of all Topics names available for that namespace (owned and accessible)
     */
    @Get
    public List<Topic> list(String namespace) {
        //TODO ?labelSelector=environment%3Dproduction,tier%3Dfrontend

        //TODO TopicList
        return topicService.findAllForNamespace(namespace);
    }

    @Get("/{topic}")
    public Optional<Topic> getTopic(String namespace, String topic) {

        Optional<Topic> optionalTopic = topicService.findByName(namespace, topic);
        if (optionalTopic.isEmpty()) {
            throw new ResourceNotFoundException();
        }
        return optionalTopic;
    }

    @Post("/")
    public Topic apply(String namespace, @Valid @Body Topic topic) {

        //TODO
        // 1. (Done) User Allowed ?
        //   -> User belongs to group and operation/resource is allowed on this namespace ?
        //   -> Managed in RessourceBasedSecurityRule class
        // 2. Topic already exists ?
        //   -> Reject Request ?
        //   -> Treat Request as and Update ?
        // 3. Request Valid ?
        //   -> Topics parameters are allowed for this namespace ConstraintsValidatorSet
        // 4. Store in datastore
        Optional<Topic> existingTopic = topicRepository.findByName(namespace, topic.getMetadata().getName());
        Namespace ns = namespaceRepository.findByName(namespace)
                .orElseThrow(() -> new RuntimeException("Namespace not found"));

        //2. Request is valid ?
        List<String> validationErrors = ns.getTopicValidator().validate(topic, ns);

        if (existingTopic.isEmpty()) {
            //Creation
            //Topic namespace ownership validation
            if (!isNamespaceOwnerOfTopic(namespace, topic.getMetadata().getName()))
                validationErrors.add("Invalid value " + topic.getMetadata().getName()
                        + " for name: Namespace not OWNER of this topic");

        } else {
            //2.2 forbidden changes when updating (partitions, replicationFactor)
            if (existingTopic.get().getSpec().getPartitions() != topic.getSpec().getPartitions()) {
                validationErrors.add("Invalid value " + topic.getSpec().getPartitions()
                        + " for configuration partitions: Value is immutable ("
                        + existingTopic.get().getSpec().getPartitions() + ")");
            }
            if (existingTopic.get().getSpec().getReplicationFactor() != topic.getSpec().getReplicationFactor()) {
                validationErrors.add("Invalid value " + topic.getSpec().getReplicationFactor()
                        + " for configuration replication.factor: Value is immutable ("
                        + existingTopic.get().getSpec().getReplicationFactor() + ")");
            }
        }
        if (validationErrors.size() > 0) {
            throw new ResourceValidationException(validationErrors);
        }

        //TODO hasChanged ?
        // if so, just return 200 with current topic, do nothing

        //3. Fill server-side fields (server side metadata + status)
        topic.getMetadata().setCluster(ns.getCluster());
        topic.getMetadata().setNamespace(ns.getName());
        topic.setStatus(Topic.TopicStatus.ofPending());
        return topicRepository.create(topic);
        //TODO quota management
        // pour les topics dont je suis owner, somme d'usage
        // pour le topic à créer usageTopic
        // si somme + usageTopic > quota KO

    }

    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{topic}")
    public HttpResponse<Void> deleteTopic(String namespace, String topic) {

        String cluster = namespaceRepository.findByName(namespace).get().getCluster();
        // allowed ?
        if (!isNamespaceOwnerOfTopic(namespace, topic))
            return HttpResponse.unauthorized();

        // exists ?
        Optional<Topic> optionalTopic = topicRepository.findByName(namespace, topic);

        if (optionalTopic.isEmpty())
            return HttpResponse.notFound();

        //1. delete from ns4kafka
        //2. delete from cluster
        topicRepository.delete(optionalTopic.get());

        //TODO cleaner delete implementation, to be discussed
        KafkaAsyncExecutor kafkaAsyncExecutor = applicationContext.getBean(KafkaAsyncExecutor.class,
                Qualifiers.byName(cluster));
        try {
            kafkaAsyncExecutor.deleteTopic(optionalTopic.get());
        } catch (Exception e) {
            //TODO refactor global error handling model
            throw new ConnectController.ConnectCreationException(e);
        }

        return HttpResponse.noContent();
    }

    private boolean isNamespaceOwnerOfTopic(String namespace, String topic) {
        return accessControlEntryRepository.findAllGrantedToNamespace(namespace).stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec()
                        .getPermission() == AccessControlEntry.Permission.OWNER)
                .filter(accessControlEntry -> accessControlEntry.getSpec()
                        .getResourceType() == AccessControlEntry.ResourceType.TOPIC)
                .anyMatch(accessControlEntry -> {
                    switch (accessControlEntry.getSpec().getResourcePatternType()) {
                    case PREFIXED:
                        return topic.startsWith(accessControlEntry.getSpec().getResource());
                    case LITERAL:
                        return topic.equals(accessControlEntry.getSpec().getResource());
                    }
                    return false;
                });
    }

    public enum TopicListLimit {
        ALL, OWNED, ACCESS_GIVEN
    }
}
