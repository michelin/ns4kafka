package com.michelin.ns4kafka.controllers.topic;

import static com.michelin.ns4kafka.utils.FormatErrorUtils.invalidNotFound;
import static com.michelin.ns4kafka.utils.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.utils.FormatErrorUtils.invalidTopicCollide;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.DeleteRecordsResponse;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import com.michelin.ns4kafka.services.TopicService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Status;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;

/**
 * Controller to manage topics.
 */
@Tag(name = "Topics", description = "Manage the topics.")
@Controller(value = "/api/namespaces/{namespace}/topics")
public class TopicController extends NamespacedResourceController {
    @Inject
    TopicService topicService;

    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * List topics by namespace.
     *
     * @param namespace The namespace
     * @return A list of topics
     */
    @Get
    public List<Topic> list(String namespace) {
        return topicService.findAllForNamespace(getNamespace(namespace));
    }

    /**
     * Get a topic by namespace and name.
     *
     * @param namespace The name
     * @param topic     The topic name
     * @return The topic
     */
    @Get("/{topic}")
    public Optional<Topic> getTopic(String namespace, String topic) {
        return topicService.findByName(getNamespace(namespace), topic);
    }

    /**
     * Create a topic.
     *
     * @param namespace The namespace
     * @param topic     The topic
     * @param dryrun    Is dry run mode or not ?
     * @return The created topic
     */
    @Post
    public HttpResponse<Topic> apply(String namespace, @Valid @Body Topic topic,
                                     @QueryValue(defaultValue = "false") boolean dryrun)
        throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = getNamespace(namespace);

        Optional<Topic> existingTopic = topicService.findByName(ns, topic.getMetadata().getName());

        // Request is valid ?
        List<String> validationErrors =
            ns.getSpec().getTopicValidator() != null ? ns.getSpec().getTopicValidator().validate(topic)
                : new ArrayList<>();

        if (existingTopic.isEmpty()) {
            // Topic namespace ownership validation
            if (!topicService.isNamespaceOwnerOfTopic(namespace, topic.getMetadata().getName())) {
                validationErrors.add(invalidOwner(topic.getMetadata().getName()));
            }

            // Topic names with a period ('.') or underscore ('_') could collide
            List<String> collidingTopics = topicService.findCollidingTopics(ns, topic);
            if (!collidingTopics.isEmpty()) {
                validationErrors.addAll(collidingTopics.stream()
                    .map(collidingTopic -> invalidTopicCollide(topic.getMetadata().getName(), collidingTopic))
                    .toList());
            }
        } else {
            validationErrors.addAll(topicService.validateTopicUpdate(ns, existingTopic.get(), topic));
        }

        topic.getSpec().getTags().replaceAll(String::toUpperCase);
        List<String> existingTags = existingTopic
            .map(oldTopic -> oldTopic.getSpec().getTags())
            .orElse(Collections.emptyList());
        if (topic.getSpec().getTags().stream().anyMatch(newTag -> !existingTags.contains(newTag))) {
            validationErrors.addAll(topicService.validateTags(ns, topic));
        }

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(Topic.kind, topic.getMetadata().getName(), validationErrors);
        }

        //3. Fill server-side fields (server side metadata + status)
        topic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        topic.getMetadata().setCluster(ns.getMetadata().getCluster());
        topic.getMetadata().setNamespace(ns.getMetadata().getName());
        topic.setStatus(Topic.TopicStatus.ofPending());

        if (existingTopic.isPresent() && existingTopic.get().equals(topic)) {
            return formatHttpResponse(existingTopic.get(), ApplyStatus.unchanged);
        }

        validationErrors.addAll(resourceQuotaService.validateTopicQuota(ns, existingTopic, topic));
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(Topic.kind, topic.getMetadata().getName(), validationErrors);
        }

        ApplyStatus status = existingTopic.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
        if (dryrun) {
            return formatHttpResponse(topic, status);
        }

        sendEventLog(Topic.kind,
            topic.getMetadata(),
            status,
            existingTopic.<Object>map(Topic::getSpec).orElse(null),
            topic.getSpec());

        return formatHttpResponse(topicService.create(topic), status);
    }

    /**
     * Delete a topic.
     *
     * @param namespace The namespace
     * @param topic     The topic
     * @param dryrun    Is dry run mode or not ?
     * @return An HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{topic}{?dryrun}")
    public HttpResponse<Void> deleteTopic(String namespace, String topic,
                                          @QueryValue(defaultValue = "false") boolean dryrun)
        throws InterruptedException, ExecutionException, TimeoutException {
        Namespace ns = getNamespace(namespace);
        if (!topicService.isNamespaceOwnerOfTopic(namespace, topic)) {
            throw new ResourceValidationException(Topic.kind, topic, invalidOwner(topic));
        }

        Optional<Topic> optionalTopic = topicService.findByName(ns, topic);

        if (optionalTopic.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        Topic topicToDelete = optionalTopic.get();
        sendEventLog(Topic.kind,
            topicToDelete.getMetadata(),
            ApplyStatus.deleted,
            topicToDelete.getSpec(),
            null);
        topicService.delete(optionalTopic.get());

        return HttpResponse.noContent();
    }

    /**
     * Import unsynchronized topics.
     *
     * @param namespace The namespace
     * @param dryrun    Is dry run mode or not ?
     * @return The list of imported topics
     * @throws ExecutionException   Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException     Any timeout exception
     */
    @Post("/_/import{?dryrun}")
    public List<Topic> importResources(String namespace, @QueryValue(defaultValue = "false") boolean dryrun)
        throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = getNamespace(namespace);
        List<Topic> unsynchronizedTopics = topicService.listUnsynchronizedTopics(ns);

        unsynchronizedTopics.forEach(topic -> {
            topic.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            topic.getMetadata().setCluster(ns.getMetadata().getCluster());
            topic.getMetadata().setNamespace(ns.getMetadata().getName());
            topic.setStatus(Topic.TopicStatus.ofSuccess("Imported from cluster"));
        });

        if (dryrun) {
            return unsynchronizedTopics;
        }

        return unsynchronizedTopics
            .stream()
            .map(topic -> {
                sendEventLog(Topic.kind, topic.getMetadata(), ApplyStatus.created, null, topic.getSpec());
                return topicService.create(topic);
            })
            .toList();
    }

    /**
     * Delete records from topic.
     *
     * @param namespace The namespace
     * @param topic     The topic
     * @param dryrun    Is dry run mode or not ?
     * @return The list of topic-partitions where records have been deleted
     * @throws ExecutionException   Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    @Post("{topic}/delete-records{?dryrun}")
    public List<DeleteRecordsResponse> deleteRecords(String namespace, String topic,
                                                     @QueryValue(defaultValue = "false") boolean dryrun)
        throws InterruptedException, ExecutionException {
        Namespace ns = getNamespace(namespace);
        if (!topicService.isNamespaceOwnerOfTopic(namespace, topic)) {
            throw new ResourceValidationException(Topic.kind, topic, invalidOwner(topic));
        }

        Optional<Topic> optionalTopic = topicService.findByName(ns, topic);
        if (optionalTopic.isEmpty()) {
            throw new ResourceValidationException(Topic.kind, topic, invalidNotFound(topic));
        }

        Topic deleteRecordsTopic = optionalTopic.get();
        List<String> validationErrors = topicService.validateDeleteRecordsTopic(deleteRecordsTopic);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(DeleteRecordsResponse.kind,
                deleteRecordsTopic.getMetadata().getName(),
                validationErrors);
        }

        Map<TopicPartition, Long> recordsToDelete = topicService.prepareRecordsToDelete(optionalTopic.get());

        Map<TopicPartition, Long> deletedRecords;
        if (dryrun) {
            deletedRecords = recordsToDelete;
        } else {
            sendEventLog(DeleteRecordsResponse.kind, optionalTopic.get().getMetadata(), ApplyStatus.deleted, null,
                null);
            deletedRecords = topicService.deleteRecords(optionalTopic.get(), recordsToDelete);
        }

        return deletedRecords.entrySet()
            .stream()
            .map(entry -> DeleteRecordsResponse.builder()
                .spec(DeleteRecordsResponse.DeleteRecordsResponseSpec.builder()
                    .topic(entry.getKey().topic())
                    .partition(entry.getKey().partition())
                    .offset(entry.getValue())
                    .build())
                .build())
            .toList();
    }
}
