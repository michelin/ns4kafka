package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.DeleteRecords;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.services.TopicService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
@Tag(name = "Topics")
@Controller(value = "/api/namespaces/{namespace}/topics")
public class TopicController extends NamespacedResourceController {
    @Inject
    TopicService topicService;

    /**
     * @param namespace The namespace to query
     * @return The list of all Topics names available for that namespace (owned and accessible)
     */
    @Get
    public List<Topic> list(String namespace) {
        log.info("List Topic received for Namespace {}",namespace);
        //TODO ?labelSelector=environment%3Dproduction,tier%3Dfrontend

        Namespace ns = getNamespace(namespace);
        //TODO TopicList
        return topicService.findAllForNamespace(ns);
    }

    @Get("/{topic}")
    public Optional<Topic> getTopic(String namespace, String topic) {
        log.info("Get Topic received for Namespace {} and for Topic {}", namespace, topic);

        Namespace ns = getNamespace(namespace);
        Optional<Topic> optionalTopic = topicService.findByName(ns, topic);

        return optionalTopic;
    }

    @Post("{?dryrun}")
    public Topic apply(String namespace, @Valid @Body Topic topic, @QueryValue(defaultValue = "false") boolean dryrun) {
        log.info("Apply Topic received for Namespace {} and for Topic {}", namespace, topic.getMetadata().getName());
        log.debug("Apply Topic received: {}", topic);

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
        Namespace ns = getNamespace(namespace);

        Optional<Topic> existingTopic = topicService.findByName(ns, topic.getMetadata().getName());

        //2. Request is valid ?
        List<String> validationErrors = ns.getSpec().getTopicValidator().validate(topic, ns);

        if (existingTopic.isEmpty()) {
            //Creation
            //Topic namespace ownership validation
            if (!topicService.isNamespaceOwnerOfTopic(namespace, topic.getMetadata().getName())) {
                validationErrors.add("Invalid value " + topic.getMetadata().getName()
                        + " for name: Namespace not OWNER of this topic");
            }
            //Topic names with a period ('.') or underscore ('_') could collide
            List<String> collidingTopics = topicService.findCollidingTopics(ns, topic);
            if (!collidingTopics.isEmpty()) {
                validationErrors.addAll(collidingTopics
                        .stream()
                        .map(collidingTopic -> "Topic " + topic.getMetadata().getName()
                                + " collides with existing topics: "
                                + collidingTopic)
                        .collect(Collectors.toList()));
            }

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
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }

        //3. Fill server-side fields (server side metadata + status)
        topic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        topic.getMetadata().setCluster(ns.getMetadata().getCluster());
        topic.getMetadata().setNamespace(ns.getMetadata().getName());
        topic.setStatus(Topic.TopicStatus.ofPending());

        if (existingTopic.isPresent() && existingTopic.get().equals(topic)) {
            return existingTopic.get();
        }

        if (dryrun) {
            return topic;
        }

        return topicService.create(topic);
    }

    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{topic}{?dryrun}")
    public HttpResponse<?> deleteTopic(String namespace, String topic, @QueryValue(defaultValue = "false") boolean dryrun) {
        log.info("Delete Topic received for Namespace {} and for Topic {}", namespace, topic);

        Namespace ns = getNamespace(namespace);

        String cluster = ns.getMetadata().getCluster();
        // allowed ?
        if (!topicService.isNamespaceOwnerOfTopic(namespace, topic))
            return HttpResponse.unauthorized();

        // exists ?
        Optional<Topic> optionalTopic = topicService.findByName(ns, topic);

        if (optionalTopic.isEmpty())
            return HttpResponse.notFound();

        if (dryrun) {
            return HttpResponse.noContent();
        }

        //1. delete from ns4kafka
        //2. delete from cluster
        topicService.delete(optionalTopic.get());

        return HttpResponse.noContent();
    }

    @Post("/_/import{?dryrun}")
    public List<Topic> importResources(String namespace, @QueryValue(defaultValue = "false") boolean dryrun)
            throws ExecutionException, InterruptedException, TimeoutException {
        log.info("Import Topic received for Namespace {}", namespace);

        Namespace ns = getNamespace(namespace);

        List<Topic> unsynchronizedTopics = topicService.listUnsynchronizedTopics(ns);

        // Augment
        unsynchronizedTopics.forEach(topic -> {
            topic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            topic.getMetadata().setCluster(ns.getMetadata().getCluster());
            topic.getMetadata().setNamespace(ns.getMetadata().getName());
            topic.setStatus(Topic.TopicStatus.ofSuccess("Imported from cluster"));
        });

        if (dryrun) {
            return unsynchronizedTopics;
        }

        List<Topic> synchronizedTopics = unsynchronizedTopics.stream()
                .map(topic -> topicService.create(topic))
                .collect(Collectors.toList());
        return synchronizedTopics;
    }

    @Post("{topic}/delete-records{?dryrun}")
    public DeleteRecords deleteRecords(String namespace, String topic, @QueryValue(defaultValue = "false") boolean dryrun) {
        log.info("Delete records received for Namespace {} and for Topic {}", namespace, topic);

        Namespace ns = getNamespace(namespace);
        // allowed ?
        if (!topicService.isNamespaceOwnerOfTopic(namespace, topic)) {
            throw new ResourceValidationException(List.of("Invalid value " + topic +
                    " for name: Namespace not OWNER of this topic"));
        }

        // exists ?
        Optional<Topic> optionalTopic = topicService.findByName(ns, topic);
        if (optionalTopic.isEmpty()) {
            throw new ResourceValidationException(List.of("Invalid value " + topic +
                    " for name: Topic doesn't exist"));
        }
        Map<TopicPartition, Long> recordsToDelete = topicService.prepareRecordsToDelete(optionalTopic.get());

        Map<TopicPartition, Long> deletedRecords;

        if (dryrun) {
            deletedRecords = recordsToDelete;
        } else {
            deletedRecords = topicService.deleteRecords(optionalTopic.get(), recordsToDelete);
        }

        return DeleteRecords.builder()
                .metadata(ObjectMeta.builder()
                        .cluster(ns.getMetadata().getCluster())
                        .namespace(namespace)
                        .name(topic)
                        .creationTimestamp(Date.from(Instant.now()))
                        .build())
                .status(DeleteRecords.DeleteRecordsStatus.builder()
                        .success(true)
                        .lowWaterMarks(deletedRecords)
                        .build())
                .build();
    }
}
