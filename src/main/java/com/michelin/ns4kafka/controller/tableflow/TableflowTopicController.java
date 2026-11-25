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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTableflowSourceTopic;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.TableflowTopic.TableflowTopicSpec;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.TableflowTopicService;
import com.michelin.ns4kafka.service.TopicService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import com.michelin.ns4kafka.validation.TableflowTopicValidator;
import com.michelin.ns4kafka.validation.ValidationResult;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.utils.SecurityService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Controller to manage the Confluent Tableflow topics. */
@Tag(name = "Tableflow Topics", description = "Manage the Confluent Tableflow topics.")
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
     * List Tableflow topics by namespace, filtered by name.
     *
     * @param namespace The namespace
     * @param name The name filter
     * @return The matching Tableflow topics
     */
    @Get
    public Flux<TableflowTopic> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        return tableflowTopicService.findByWildcardName(getNamespace(namespace), name);
    }

    /**
     * Create or update a Tableflow topic.
     *
     * @param namespace The namespace
     * @param tableflowTopic The Tableflow topic
     * @param dryrun Is dry run mode or not?
     * @return The applied Tableflow topic
     */
    @Post
    public Mono<HttpResponse<TableflowTopic>> apply(
            String namespace,
            @Valid @Body TableflowTopic tableflowTopic,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        String topic = tableflowTopic.getMetadata().getName();
        if (!topicService.isNamespaceOwnerOfTopic(namespace, topic)) {
            return Mono.error(new ResourceValidationException(tableflowTopic, invalidOwner(topic)));
        }

        TableflowTopicSpec validatedExistingSpec = tableflowTopicService
                .findByName(ns, topic)
                .map(TableflowTopic::getSpec)
                .orElse(null);
        TableflowTopicValidator validator =
                Objects.requireNonNullElseGet(ns.getSpec().getTableflowTopicValidator(), TableflowTopicValidator::new);
        ValidationResult specValidation = validator.validate(tableflowTopic, validatedExistingSpec);
        List<String> validationErrors = new ArrayList<>(specValidation.errors());
        validationErrors.addAll(tableflowTopicService.validateConfiguration(ns, topic));

        Optional<Topic> existingTopic = topicService.findByName(ns, topic);
        if (existingTopic.isEmpty()) {
            validationErrors.add(invalidTableflowSourceTopic(topic));
        }

        if (!validationErrors.isEmpty()) {
            return Mono.error(new ResourceValidationException(tableflowTopic, validationErrors));
        }

        return tableflowTopicService
                .validateSchemas(existingTopic.get())
                // Ns4Kafka storage is blocking; keep it outside the HTTP client event loop.
                .publishOn(Schedulers.boundedElastic())
                .map(errors -> {
                    if (!errors.isEmpty()) {
                        throw new ResourceValidationException(tableflowTopic, errors);
                    }

                    Optional<TableflowTopic> existingTableflowTopic = tableflowTopicService.findByName(ns, topic);
                    TableflowTopicSpec existingSpec =
                            existingTableflowTopic.map(TableflowTopic::getSpec).orElse(null);
                    // A concurrent apply can change the stored spec while schema validation is in flight.
                    ValidationResult validation = Objects.equals(validatedExistingSpec, existingSpec)
                            ? specValidation
                            : validator.validate(tableflowTopic, existingSpec);
                    if (validation.hasErrors()) {
                        throw new ResourceValidationException(tableflowTopic, validation.errors());
                    }
                    List<String> validationWarnings = validation.warnings();
                    Long retention = tableflowTopic.getSpec().getSnapshotRetentionMs();
                    if (existingSpec != null
                            && (retention == null || retention.equals(existingSpec.getSnapshotRetentionMs()))) {
                        return formatHttpResponse(
                                existingTableflowTopic.orElseThrow(), ApplyStatus.UNCHANGED, validationWarnings);
                    }

                    assignResourceMetadata(tableflowTopic, ns, existingTableflowTopic.orElse(null));
                    ApplyStatus status = existingSpec == null ? ApplyStatus.CREATED : ApplyStatus.CHANGED;
                    if (dryrun) {
                        return formatHttpResponse(tableflowTopic, status, validationWarnings);
                    }

                    TableflowTopic appliedTableflowTopic = tableflowTopicService.create(tableflowTopic);
                    sendEventLog(
                            appliedTableflowTopic, status, existingSpec, appliedTableflowTopic.getSpec(), EMPTY_STRING);

                    return formatHttpResponse(appliedTableflowTopic, status, validationWarnings);
                });
    }

    /**
     * Import unsynchronized Tableflow topics.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @param dryrun Is dry run mode or not?
     * @return The list of imported Tableflow topics
     */
    @Post("/_/import")
    public Mono<List<TableflowTopic>> importResources(
            String namespace,
            @QueryValue(defaultValue = "*") String name,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);
        return tableflowTopicService
                .listUnsynchronizedTableflowTopicsByWildcardName(ns, name)
                .map(unsynchronizedTableflowTopic -> {
                    assignResourceMetadata(unsynchronizedTableflowTopic, ns, null);
                    unsynchronizedTableflowTopic.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                    unsynchronizedTableflowTopic.getMetadata().setGeneration(1);

                    if (dryrun) {
                        return unsynchronizedTableflowTopic;
                    }

                    TableflowTopic importedTableflowTopic = tableflowTopicService.create(unsynchronizedTableflowTopic);
                    sendEventLog(
                            importedTableflowTopic,
                            ApplyStatus.CREATED,
                            null,
                            importedTableflowTopic.getSpec(),
                            EMPTY_STRING);

                    return importedTableflowTopic;
                })
                .collectList();
    }
}
