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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTableflowValueSchema;
import static com.michelin.ns4kafka.util.enumation.Kind.TABLEFLOW_TOPIC;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;
import reactor.core.publisher.Mono;

/** Service to validate and create Tableflow resources. */
@Singleton
public class TableflowTopicService {
    private final TableflowTopicRepository tableflowTopicRepository;
    private final SchemaRegistryClient schemaRegistryClient;
    private final List<ManagedClusterProperties> managedClusterProperties;

    /**
     * Constructor.
     *
     * @param tableflowTopicRepository The Tableflow topic repository
     * @param schemaRegistryClient The Schema Registry client
     * @param managedClusterProperties The managed cluster properties
     */
    public TableflowTopicService(
            TableflowTopicRepository tableflowTopicRepository,
            SchemaRegistryClient schemaRegistryClient,
            List<ManagedClusterProperties> managedClusterProperties) {
        this.tableflowTopicRepository = tableflowTopicRepository;
        this.schemaRegistryClient = schemaRegistryClient;
        this.managedClusterProperties = managedClusterProperties;
    }

    /**
     * Validate the local cluster configuration before looking up the topic and schemas.
     *
     * @param namespace The namespace
     * @param topic The source topic name
     */
    public void validateConfiguration(Namespace namespace, String topic) {
        Optional<ManagedClusterProperties> cluster = managedClusterProperties.stream()
                .filter(properties ->
                        properties.getName().equals(namespace.getMetadata().getCluster()))
                .findFirst();
        if (cluster.isEmpty() || !cluster.get().isConfluentCloud()) {
            throw new ResourceValidationException(
                    TABLEFLOW_TOPIC, topic, "Tableflow is supported only for Confluent Cloud clusters.");
        }

        ManagedClusterProperties.ConfluentCloudProperties config = cluster.get().getConfluentCloud();
        if (config == null
                || StringUtils.isEmpty(config.getClusterId())
                || StringUtils.isEmpty(config.getEnvironmentId())
                || StringUtils.isEmpty(config.getBasicAuthUsername())
                || StringUtils.isEmpty(config.getBasicAuthPassword())) {
            throw new ResourceValidationException(
                    TABLEFLOW_TOPIC,
                    topic,
                    "Tableflow creation requires cluster ID, environment ID, and Confluent Cloud credentials.");
        }
    }

    /**
     * Check the latest value schema under TopicNameStrategy before enabling Tableflow.
     *
     * @param topic The source Kafka topic
     * @return The validation errors
     */
    public Mono<List<String>> validateSchemas(Topic topic) {
        String subject = topic.getMetadata().getName() + "-value";
        return schemaRegistryClient
                .getSubject(topic.getMetadata().getCluster(), subject, "latest")
                .filter(schema -> schema.schema() != null && !schema.schema().isBlank())
                .map(_ -> List.<String>of())
                .defaultIfEmpty(List.of(invalidTableflowValueSchema(subject)));
    }

    /**
     * Find a Tableflow topic in the namespace's managed cluster.
     *
     * @param namespace The namespace
     * @param name The source topic name
     * @return The stored Tableflow topic
     */
    public Optional<TableflowTopic> findByName(Namespace namespace, String name) {
        return tableflowTopicRepository.findByName(namespace.getMetadata().getCluster(), name);
    }

    /**
     * Store a Tableflow configuration for asynchronous deployment.
     *
     * @param tableflowTopic The Tableflow topic to store
     * @return The stored Tableflow topic
     */
    public TableflowTopic create(TableflowTopic tableflowTopic) {
        return tableflowTopicRepository.create(tableflowTopic);
    }
}
