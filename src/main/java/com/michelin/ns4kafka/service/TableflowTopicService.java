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
package com.michelin.ns4kafka.service;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.service.executor.TopicAsyncExecutor;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.json.tree.JsonNode;
import io.netty.channel.ConnectTimeoutException;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

/** Service to validate and create Tableflow resources. */
@Singleton
public class TableflowTopicService {
    private final ApplicationContext applicationContext;
    private final SchemaRegistryClient schemaRegistryClient;

    /**
     * Constructor.
     *
     * @param applicationContext The application context
     * @param schemaRegistryClient The Schema Registry client
     */
    public TableflowTopicService(ApplicationContext applicationContext, SchemaRegistryClient schemaRegistryClient) {
        this.applicationContext = applicationContext;
        this.schemaRegistryClient = schemaRegistryClient;
    }

    /**
     * Check the latest key and value schemas under TopicNameStrategy before enabling Tableflow.
     *
     * @param topic The source Kafka topic
     * @return The validation errors
     */
    public List<String> validateSchemas(Topic topic) {
        List<String> errors = new ArrayList<>();
        try {
            for (String suffix : List.of("-key", "-value")) {
                String subject = topic.getMetadata().getName() + suffix;
                var schema = schemaRegistryClient
                        .getSubject(topic.getMetadata().getCluster(), subject, "latest")
                        .block();
                if (schema == null || schema.schema() == null || schema.schema().isBlank()) {
                    errors.add("Tableflow requires a registered schema for subject [" + subject + "].");
                }
            }
        } catch (ResourceValidationException exception) {
            throw exception;
        } catch (HttpClientResponseException exception) {
            throw new HttpStatusException(
                    HttpStatus.BAD_GATEWAY,
                    "Schema Registry validation returned HTTP " + exception.code()
                            + ". Check Schema Registry access and availability. Tableflow creation was not attempted.");
        } catch (RuntimeException exception) {
            boolean timeout = false;
            for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                timeout |= cause instanceof ReadTimeoutException
                        || cause instanceof ConnectTimeoutException
                        || cause instanceof java.util.concurrent.TimeoutException
                        || cause instanceof java.net.SocketTimeoutException;
            }
            throw new HttpStatusException(
                    timeout ? HttpStatus.GATEWAY_TIMEOUT : HttpStatus.BAD_GATEWAY,
                    "Schema Registry validation " + (timeout ? "timed out" : "failed")
                            + ". Check Schema Registry access and availability. Tableflow creation was not attempted.");
        }
        return errors;
    }

    /**
     * Create a Tableflow topic through the existing cluster-specific topic executor.
     *
     * @param tableflowTopic The Tableflow topic to create
     * @return The Tableflow topic with the Confluent identifier and creation status
     */
    public TableflowTopic create(TableflowTopic tableflowTopic) {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(
                TopicAsyncExecutor.class,
                Qualifiers.byName(tableflowTopic.getMetadata().getCluster()));
        JsonNode response = topicAsyncExecutor.createTableflowTopic(tableflowTopic);
        tableflowTopic.setStatus(TableflowTopic.TableflowTopicStatus.builder()
                .id(response.get("id") == null ? null : response.get("id").getStringValue())
                .state(response.get("status"))
                .build());
        tableflowTopic
                .getMetadata()
                .setStatus(Resource.Metadata.Status.builder()
                        .phase(Resource.Metadata.Phase.PENDING)
                        .message("Creation accepted by Confluent; table materialization is asynchronous.")
                        .build());
        return tableflowTopic;
    }
}
