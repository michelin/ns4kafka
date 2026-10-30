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
package com.michelin.ns4kafka.controller.tableflow;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNotFound;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.util.enumation.Kind.TABLEFLOW_TOPIC;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.TableflowTopicService;
import com.michelin.ns4kafka.service.TopicService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import com.michelin.ns4kafka.validation.ValidationResult;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.utils.SecurityService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Controller to manage Tableflow resources independently from Kafka topics. */
@Tag(name = "Tableflow Topics", description = "Enable Tableflow for existing Kafka topics.")
@Controller(value = "/api/namespaces/{namespace}/tableflow-topics")
@ExecuteOn(TaskExecutors.IO)
public class TableflowTopicController extends NamespacedResourceController {
    private final TopicService topicService;
    private final TableflowTopicService tableflowTopicService;

    /**
     * Constructor.
     *
     * @param topicService The topic service
     * @param tableflowTopicService The Tableflow topic service
     * @param namespaceService The namespace service
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    protected TableflowTopicController(
            TopicService topicService,
            TableflowTopicService tableflowTopicService,
            NamespaceService namespaceService,
            SecurityService securityService,
            ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        super(namespaceService, securityService, applicationEventPublisher);
        this.topicService = topicService;
        this.tableflowTopicService = tableflowTopicService;
    }

    /**
     * Create a Tableflow resource for the Kafka topic named in metadata.name.
     *
     * @param namespace The namespace
     * @param tableflowTopic The Tableflow topic
     * @param dryrun Is dry run mode or not?
     * @return The Tableflow topic accepted for creation, or its dry-run preview
     */
    @Post
    @Status(HttpStatus.ACCEPTED)
    public HttpResponse<TableflowTopic> apply(
            String namespace,
            @Valid @Body TableflowTopic tableflowTopic,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        String topic = tableflowTopic.getMetadata().getName();
        if (!topicService.isNamespaceOwnerOfTopic(namespace, topic)) {
            throw new ResourceValidationException(tableflowTopic, invalidOwner(topic));
        }

        List<String> validationErrors = new ArrayList<>();
        List<String> validationWarnings = new ArrayList<>();
        if (ns.getSpec().getTableflowTopicValidator() != null) {
            ValidationResult validationResult =
                    ns.getSpec().getTableflowTopicValidator().validate(tableflowTopic);
            validationErrors.addAll(validationResult.errors());
            validationWarnings.addAll(validationResult.warnings());
        }
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(tableflowTopic, validationErrors);
        }

        Topic existingTopic = topicService
                .findByName(ns, topic)
                .orElseThrow(() -> new ResourceValidationException(tableflowTopic, invalidNotFound(topic)));
        if (!existingTopic.isCreated()
                || (existingTopic.getMetadata().getStatus() != null
                        && existingTopic.getMetadata().getStatus().getPhase() == Resource.Metadata.Phase.DELETING)) {
            throw new ResourceValidationException(
                    tableflowTopic, "Tableflow requires a deployed topic that is not deleting.");
        }
        validationErrors = tableflowTopicService.validateSchemas(existingTopic);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(tableflowTopic, validationErrors);
        }

        Optional<TableflowTopic> existingTableflowTopic = tableflowTopicService.findByName(ns, topic);
        if (existingTableflowTopic.isPresent()
                && !existingTableflowTopic.get().getSpec().equals(tableflowTopic.getSpec())) {
            throw new ResourceValidationException(
                    tableflowTopic, "Updating Tableflow configuration is not supported yet.");
        }

        assignResourceMetadata(tableflowTopic, ns, existingTableflowTopic.orElse(null));
        tableflowTopic.setApiVersion("v1");
        tableflowTopic.setKind(TABLEFLOW_TOPIC);
        tableflowTopic.setStatus(null);

        if (existingTableflowTopic.isPresent()) {
            return formatHttpResponse(existingTableflowTopic.get(), ApplyStatus.UNCHANGED, validationWarnings);
        }

        if (dryrun) {
            return formatHttpResponse(tableflowTopic, ApplyStatus.CREATED, validationWarnings);
        }

        TableflowTopic createdTableflowTopic = tableflowTopicService.create(tableflowTopic);
        sendEventLog(createdTableflowTopic, ApplyStatus.CREATED, null, createdTableflowTopic.getSpec(), EMPTY_STRING);

        return formatHttpResponse(createdTableflowTopic, ApplyStatus.CREATED, validationWarnings)
                .toMutableResponse()
                .body(createdTableflowTopic)
                .status(HttpStatus.ACCEPTED);
    }
}
