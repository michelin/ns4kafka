package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.exceptions.ResourceValidationException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.hateoas.JsonError;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Getter;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
@Tag(name = "Topics")
@Controller(value = "/api/namespaces/{namespace}/topics")
public class TopicController {
    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    TopicRepository topicRepository;

    /**
     * @param namespace The namespace to query
     * @param limit Optional restricts the scope of the return list
     * @return The list of all Topics names available for that namespace (owned and accessible)
     */
    @Get
    public List<String> list(String namespace, @Nullable @QueryValue TopicListLimit limit){
        //TODO ?labelSelector=environment%3Dproduction,tier%3Dfrontend

        if(limit==null){
            limit=TopicListLimit.ALL;
        }
        //TODO TopicList
        return topicRepository.findAllForNamespace(namespace, limit)
                .stream()
                .map(topic -> topic.getMetadata().getName())
                .collect(Collectors.toList());
    }

    @Get("/{topic}")
    public Optional<Topic> getTopic(String namespace, String topic){
        return topicRepository.findByName(namespace, topic);
    }
    @Put("/{topicName}")
    public Topic update(String namespace, String topicName, @Body Topic topic){

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
        Optional<Topic> existingTopic = topicRepository.findByName(namespace,topic.getMetadata().getName());
        if(existingTopic.isPresent()){
            update(namespace, topic.getMetadata().getName(), topic);
        }
        Namespace ns = namespaceRepository.findByName(namespace).orElseThrow(() -> new RuntimeException("Namespace not found"));

        //2. Request is valid ?
        //TODO forbidden changes when updating (partitions, replicationFactor, other ?)
        List<String> validationErrors = ns.getTopicValidator().validate(topic,ns);
        if(validationErrors.size()>0){
            throw new ResourceValidationException(validationErrors);
        }

        //3. Fill server-side fields (server side metadata + status)
        topic.setApiVersion("v1");
        topic.setKind("Topic");
        topic.getMetadata().setCluster(ns.getCluster());
        topic.getMetadata().setNamespace(ns.getName());
        topic.setStatus(Topic.TopicStatus.ofPending());
        return topicRepository.create(topic);
        //pour les topics dont je suis owner, somme d'usage
        // pour le topic à créer usageTopic
        // si somme + usageTopic > quota KO
    }
    @Post("/")
    public Topic create(String namespace, @Body Topic topic){

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
        Optional<Topic> existingTopic = topicRepository.findByName(namespace,topic.getMetadata().getName());
        if(existingTopic.isPresent()){
            update(namespace, topic.getMetadata().getName(), topic);
        }
        Namespace ns = namespaceRepository.findByName(namespace).orElseThrow(() -> new RuntimeException("Namespace not found"));

        //2. Request is valid ?
        List<String> validationErrors = ns.getTopicValidator().validate(topic,ns);
        if(validationErrors.size()>0){
            throw new ResourceValidationException(validationErrors);
        }

        //3. Fill server-side fields (server side metadata + status)
        topic.setApiVersion("v1");
        topic.setKind("Topic");
        topic.getMetadata().setCluster(ns.getCluster());
        topic.getMetadata().setNamespace(ns.getName());
        topic.setStatus(Topic.TopicStatus.ofPending());
        return topicRepository.create(topic);
        //pour les topics dont je suis owner, somme d'usage
        // pour le topic à créer usageTopic
        // si somme + usageTopic > quota KO

    }
    @Error
    public HttpResponse<ResourceCreationError> validationExceptionHandler(HttpRequest request, ResourceValidationException resourceValidationException){
        return HttpResponse
                .badRequest()
                .body(new ResourceCreationError("Message validation failed", resourceValidationException.getValidationErrors()));
    }


    public static class ResourceCreationError extends JsonError {
        @Getter
        List<String> validationErrors;
        public ResourceCreationError(String message, List<String> validationErrors) {
            super(message);
            this.validationErrors=validationErrors;
        }
    }

    public enum TopicListLimit {
        ALL,
        OWNED,
        ACCESS_GIVEN
    }
}
