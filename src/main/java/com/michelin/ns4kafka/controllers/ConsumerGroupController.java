package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.consumer.group.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.consumer.group.ConsumerGroupResetOffsetsResponse;
import com.michelin.ns4kafka.services.ConsumerGroupService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Tag(name = "Consumer Groups")
@Controller("/api/namespaces/{namespace}/consumer-groups")
public class ConsumerGroupController extends NamespacedResourceController {
    /**
     * The consumer group service
     */
    @Inject
    ConsumerGroupService consumerGroupService;

    /**
     * Reset offsets for a given topic and consumer group
     * @param namespace The namespace
     * @param consumerGroup The consumer group
     * @param consumerGroupResetOffsets The information about how to reset
     * @param dryrun Is dry run mode or not ?
     * @return The reset offsets response
     */
    @Post("/{consumerGroup}/reset{?dryrun}")
    public List<ConsumerGroupResetOffsetsResponse> resetOffsets(String namespace, String consumerGroup,
                                                                @Valid @Body ConsumerGroupResetOffsets consumerGroupResetOffsets,
                                                                @QueryValue(defaultValue = "false") boolean dryrun) throws ExecutionException {
        Namespace ns = getNamespace(namespace);

        // Validate spec
        List<String> validationErrors = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);

        // Validate ownership
        if (!consumerGroupService.isNamespaceOwnerOfConsumerGroup(namespace, consumerGroup)) {
            validationErrors.add("Namespace not owner of this consumer group \"" + consumerGroup + "\".");
        }

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, "ConsumerGroup", consumerGroup);
        }

        // Augment
        consumerGroupResetOffsets.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        consumerGroupResetOffsets.getMetadata().setNamespace(ns.getMetadata().getName());
        consumerGroupResetOffsets.getMetadata().setCluster(ns.getMetadata().getCluster());

        List<ConsumerGroupResetOffsetsResponse> topicPartitionOffsets = null;
        try {
            // Starting from here, all the code is from Kafka kafka-consumer-group command
            // https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/admin/ConsumerGroupCommand.scala#L421
            // Validate Consumer Group is dead or inactive
            String currentState = consumerGroupService.getConsumerGroupStatus(ns, consumerGroup);
            if (!List.of("Empty", "Dead").contains(currentState)) {
                throw new IllegalStateException("Assignments can only be reset if the consumer group \"" + consumerGroup + "\" is inactive, but the current state is " + currentState.toLowerCase() + ".");
            }

            // List partitions
            List<TopicPartition> partitionsToReset = consumerGroupService.getPartitionsToReset(ns, consumerGroup, consumerGroupResetOffsets.getSpec().getTopic());

            // Prepare offsets
            Map<TopicPartition, Long> preparedOffsets = consumerGroupService.prepareOffsetsToReset(ns, consumerGroup, consumerGroupResetOffsets.getSpec().getOptions(), partitionsToReset, consumerGroupResetOffsets.getSpec().getMethod());

            if (!dryrun) {
                sendEventLog("ConsumerGroupResetOffsets",
                        consumerGroupResetOffsets.getMetadata(),
                        ApplyStatus.changed,
                        null,
                        consumerGroupResetOffsets.getSpec());
                consumerGroupService.alterConsumerGroupOffsets(ns, consumerGroup, preparedOffsets);
            }

            topicPartitionOffsets = preparedOffsets.entrySet()
                    .stream()
                    .map(entry -> ConsumerGroupResetOffsetsResponse.builder()
                            .spec(ConsumerGroupResetOffsetsResponse.ConsumerGroupResetOffsetsResponseSpec.builder()
                                    .topic(entry.getKey().topic())
                                    .partition(entry.getKey().partition())
                                    .offset(entry.getValue())
                                    .consumerGroup(consumerGroup)
                                    .build())
                            .build())
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return topicPartitionOffsets;
    }
}
