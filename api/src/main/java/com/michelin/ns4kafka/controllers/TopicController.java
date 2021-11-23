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
import org.apache.kafka.common.TopicPartition;

import jakarta.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
        //TODO ?labelSelector=environment%3Dproduction,tier%3Dfrontend

        Namespace ns = getNamespace(namespace);
        //TODO TopicList
        return topicService.findAllForNamespace(ns);
    }

    @Get("/{topic}")
    public Optional<Topic> getTopic(String namespace, String topic) {

        Namespace ns = getNamespace(namespace);
        Optional<Topic> optionalTopic = topicService.findByName(ns, topic);

        return optionalTopic;
    }

    @Post("{?dryrun}")
    public HttpResponse<Topic> apply(String namespace, @Valid @Body Topic topic, @QueryValue(defaultValue = "false") boolean dryrun) throws InterruptedException, ExecutionException, TimeoutException {

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
                validationErrors.addAll(collidingTopics.stream()
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
            throw new ResourceValidationException(validationErrors, topic.getKind(), topic.getMetadata().getName());
        }

        //3. Fill server-side fields (server side metadata + status)
        topic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        topic.getMetadata().setCluster(ns.getMetadata().getCluster());
        topic.getMetadata().setNamespace(ns.getMetadata().getName());
        topic.setStatus(Topic.TopicStatus.ofPending());

        if (existingTopic.isPresent() && existingTopic.get().equals(topic)) {
            return formatHttpResponse(existingTopic.get(), ApplyStatus.unchanged);
        }
        ApplyStatus status = existingTopic.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

        if (dryrun) {
            return formatHttpResponse(topic, status);
        }
        sendEventLog(topic.getKind(),
                topic.getMetadata(),
                status,
                existingTopic.isPresent() ? existingTopic.get().getSpec(): null,
                topic.getSpec());

        return formatHttpResponse(topicService.create(topic), status);
    }

    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{topic}{?dryrun}")
    public HttpResponse deleteTopic(String namespace, String topic, @QueryValue(defaultValue = "false") boolean dryrun) throws InterruptedException, ExecutionException, TimeoutException {

        Namespace ns = getNamespace(namespace);

        // allowed ?
        if (!topicService.isNamespaceOwnerOfTopic(namespace, topic))
            throw new ResourceValidationException(List.of("Invalid value " + topic
                    + " for name: Namespace not OWNER of this topic"), "Topic", topic);

        // exists ?
        Optional<Topic> optionalTopic = topicService.findByName(ns, topic);

        if (optionalTopic.isEmpty())
            return HttpResponse.notFound();

        if (dryrun) {
            return HttpResponse.noContent();
        }
        Topic topicToDelete = optionalTopic.get();
        sendEventLog(topicToDelete.getKind(),
                topicToDelete.getMetadata(),
                ApplyStatus.deleted,
                topicToDelete.getSpec(),
                null);
        // delete from cluster
        topicService.delete(optionalTopic.get());

        return HttpResponse.noContent();
    }

    @Post("/_/import{?dryrun}")
    public List<Topic> importResources(String namespace, @QueryValue(defaultValue = "false") boolean dryrun)
            throws ExecutionException, InterruptedException, TimeoutException {

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
                .map(topic -> {
                    sendEventLog("Topic", topic.getMetadata(), ApplyStatus.created, null, topic.getSpec());
                    return topicService.create(topic);
                })
                .collect(Collectors.toList());
        return synchronizedTopics;
    }

    @Post("{topic}/delete-records{?dryrun}")
    public DeleteRecords deleteRecords(String namespace, String topic, @QueryValue(defaultValue = "false") boolean dryrun) throws InterruptedException, ExecutionException {

        Namespace ns = getNamespace(namespace);
        // allowed ?
        if (!topicService.isNamespaceOwnerOfTopic(namespace, topic)) {
            throw new ResourceValidationException(List.of("Invalid value " + topic
                    + " for name: Namespace not OWNER of this topic"), "Topic", topic);
        }

        // exists ?
        Optional<Topic> optionalTopic = topicService.findByName(ns, topic);
        if (optionalTopic.isEmpty()) {
            throw new ResourceValidationException(
                    List.of("Invalid value " + topic +
                    " for name: Topic doesn't exist"),
                    "Topic",
                    topic
            );
        }
        Map<TopicPartition, Long> recordsToDelete = topicService.prepareRecordsToDelete(optionalTopic.get());

        Map<TopicPartition, Long> deletedRecords;

        if (dryrun) {
            deletedRecords = recordsToDelete;
        } else {
            sendEventLog("Topic", optionalTopic.get().getMetadata(), ApplyStatus.deleted, null, null);
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
                        .lowWaterMarks(
                                deletedRecords.entrySet()
                                        .stream()
                                        .collect(Collectors.toMap(k -> k.getKey().toString(), Map.Entry::getValue))
                        )
                        .build())
                .build();
    }
}
