/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConsumerGroupDeleteOperation;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConsumerGroupOperation;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.util.enumation.Kind.CONSUMER_GROUP;
import static com.michelin.ns4kafka.util.enumation.Kind.CONSUMER_GROUP_RESET_OFFSET;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsetsResponse;
import com.michelin.ns4kafka.service.ConsumerGroupService;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.security.utils.SecurityService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;

/** Controller to manage the consumer groups. */
@Tag(name = "Consumer Groups", description = "Manage the consumer groups.")
@Controller("/api/namespaces/{namespace}/consumer-groups")
public class ConsumerGroupController extends NamespacedResourceController {
    private final ConsumerGroupService consumerGroupService;

    /**
     * Constructor.
     *
     * @param namespaceService The namespace service
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    public ConsumerGroupController(
            ConsumerGroupService consumerGroupService,
            NamespaceService namespaceService,
            SecurityService securityService,
            ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        super(namespaceService, securityService, applicationEventPublisher);
        this.consumerGroupService = consumerGroupService;
    }

    /**
     * Reset offsets by topic and consumer group.
     *
     * @param namespace The namespace
     * @param consumerGroup The consumer group
     * @param consumerGroupResetOffsets The information about how to reset
     * @param dryrun Is dry run mode or not?
     * @return The reset offsets response
     */
    @Post("/{consumerGroup}/reset{?dryrun}")
    public List<ConsumerGroupResetOffsetsResponse> resetOffsets(
            String namespace,
            String consumerGroup,
            @Valid @Body ConsumerGroupResetOffsets consumerGroupResetOffsets,
            @QueryValue(defaultValue = "false") boolean dryrun)
            throws ExecutionException {
        List<String> validationErrors = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);

        if (!consumerGroupService.isNamespaceOwnerOfConsumerGroup(getNamespace(namespace), consumerGroup)) {
            validationErrors.add(invalidOwner("group", consumerGroup));
        }

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(CONSUMER_GROUP_RESET_OFFSET, consumerGroup, validationErrors);
        }

        Namespace ns = getNamespace(namespace);
        consumerGroupResetOffsets.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        consumerGroupResetOffsets.getMetadata().setNamespace(ns.getMetadata().getName());
        consumerGroupResetOffsets.getMetadata().setCluster(ns.getMetadata().getCluster());

        List<ConsumerGroupResetOffsetsResponse> topicPartitionOffsets = null;
        try {
            // Validate Consumer Group is empty or dead
            // The check for DEAD state is for retro compatibility with Kafka server 3.X
            GroupState currentState = consumerGroupService.getConsumerGroupStatus(ns, consumerGroup);
            if (!List.of(GroupState.EMPTY, GroupState.DEAD).contains(currentState)) {
                throw new ResourceValidationException(
                        CONSUMER_GROUP_RESET_OFFSET,
                        consumerGroup,
                        invalidConsumerGroupOperation(
                                consumerGroup,
                                GroupState.EMPTY.toString().toLowerCase(),
                                currentState.toString().toLowerCase()));
            }

            // List partitions
            List<TopicPartition> partitionsToReset = consumerGroupService.getPartitionsToReset(
                    ns, consumerGroup, consumerGroupResetOffsets.getSpec().getTopic());

            // Prepare offsets
            Map<TopicPartition, Long> preparedOffsets = consumerGroupService.prepareOffsetsToReset(
                    ns,
                    consumerGroup,
                    consumerGroupResetOffsets.getSpec().getOptions(),
                    partitionsToReset,
                    consumerGroupResetOffsets.getSpec().getMethod());

            if (!dryrun) {
                sendEventLog(
                        consumerGroupResetOffsets,
                        ApplyStatus.CHANGED,
                        null,
                        consumerGroupResetOffsets.getSpec(),
                        EMPTY_STRING);

                consumerGroupService.alterConsumerGroupOffsets(ns, consumerGroup, preparedOffsets);
            }

            topicPartitionOffsets = preparedOffsets.entrySet().stream()
                    .map(entry -> ConsumerGroupResetOffsetsResponse.builder()
                            .spec(ConsumerGroupResetOffsetsResponse.ConsumerGroupResetOffsetsResponseSpec.builder()
                                    .topic(entry.getKey().topic())
                                    .partition(entry.getKey().partition())
                                    .offset(entry.getValue())
                                    .consumerGroup(consumerGroup)
                                    .build())
                            .build())
                    .toList();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }

        return topicPartitionOffsets;
    }

    /**
     * Delete a consumer group.
     *
     * @param namespace The namespace
     * @param consumerGroup The consumer group
     * @param dryrun Is dry run mode or not?
     * @return An HTTP response
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    @Delete("/{consumerGroup}{?dryrun}")
    public HttpResponse<Void> deleteConsumerGroup(
            String namespace, String consumerGroup, @QueryValue(defaultValue = "false") boolean dryrun)
            throws ExecutionException, InterruptedException {
        Namespace ns = getNamespace(namespace);

        if (!consumerGroupService.isNamespaceOwnerOfConsumerGroup(ns, consumerGroup)) {
            throw new ResourceValidationException(CONSUMER_GROUP, consumerGroup, invalidOwner("group", consumerGroup));
        }

        GroupState currentState = consumerGroupService.getConsumerGroupStatus(ns, consumerGroup);

        if (currentState == GroupState.DEAD) {
            return HttpResponse.notFound();
        }

        if (currentState != GroupState.EMPTY) {
            throw new ResourceValidationException(
                    CONSUMER_GROUP,
                    consumerGroup,
                    invalidConsumerGroupDeleteOperation(
                            consumerGroup,
                            GroupState.EMPTY.toString().toLowerCase(),
                            currentState.toString().toLowerCase()));
        }

        if (!dryrun) {
            consumerGroupService.deleteConsumerGroup(ns, consumerGroup);
        }
        return HttpResponse.noContent();
    }
}
