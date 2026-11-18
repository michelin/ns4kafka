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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTableflowClusterConfiguration;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTableflowValueSchema;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.util.RegexUtils;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Service to manage Tableflow topics. */
@Singleton
public class TableflowTopicService {
    private final AclService aclService;
    private final ConfluentCloudClient confluentCloudClient;
    private final TableflowTopicRepository tableflowTopicRepository;
    private final SchemaRegistryClient schemaRegistryClient;
    private final List<ManagedClusterProperties> managedClusterProperties;

    /**
     * Constructor.
     *
     * @param aclService The ACL service
     * @param confluentCloudClient The Confluent Cloud client
     * @param tableflowTopicRepository The Tableflow topic repository
     * @param schemaRegistryClient The Schema Registry client
     * @param managedClusterProperties The managed cluster properties
     */
    public TableflowTopicService(
            AclService aclService,
            ConfluentCloudClient confluentCloudClient,
            TableflowTopicRepository tableflowTopicRepository,
            SchemaRegistryClient schemaRegistryClient,
            List<ManagedClusterProperties> managedClusterProperties) {
        this.aclService = aclService;
        this.confluentCloudClient = confluentCloudClient;
        this.tableflowTopicRepository = tableflowTopicRepository;
        this.schemaRegistryClient = schemaRegistryClient;
        this.managedClusterProperties = managedClusterProperties;
    }

    /**
     * Find all Tableflow topics of a given namespace.
     *
     * @param namespace The namespace
     * @return The Tableflow topics
     */
    public List<TableflowTopic> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls =
                aclService.findResourceOwnerGrantedToNamespace(namespace, AccessControlEntry.ResourceType.TOPIC);
        return tableflowTopicRepository
                .findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(tableflowTopic -> aclService.isResourceCoveredByAcls(
                        acls, tableflowTopic.getMetadata().getName()))
                .toList();
    }

    /**
     * Find all Tableflow topics of a given namespace, filtered by name, with their current Confluent Cloud status.
     *
     * @param namespace The namespace
     * @param name The name filter
     * @return The Tableflow topics
     */
    public Flux<TableflowTopic> findByWildcardName(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        List<TableflowTopic> tableflowTopics = findAllForNamespace(namespace).stream()
                .filter(tableflowTopic -> RegexUtils.isResourceCoveredByRegex(
                        tableflowTopic.getMetadata().getName(), nameFilterPatterns))
                .toList();

        if (tableflowTopics.isEmpty()
                || managedClusterProperties.stream()
                        .filter(properties -> properties
                                .getName()
                                .equals(namespace.getMetadata().getCluster()))
                        .noneMatch(ManagedClusterProperties::isManageTableflow)) {
            return Flux.fromIterable(tableflowTopics);
        }

        return confluentCloudClient
                .listTableflowTopics(namespace.getMetadata().getCluster())
                .filter(response -> response.spec() != null)
                .collectMap(response -> response.spec().displayName())
                .onErrorReturn(Map.of())
                .flatMapIterable(responses -> tableflowTopics.stream()
                        .map(tableflowTopic -> {
                            TableflowTopicResponse response =
                                    responses.get(tableflowTopic.getMetadata().getName());
                            if (response == null) {
                                return tableflowTopic;
                            }

                            return TableflowTopic.builder()
                                    .metadata(tableflowTopic.getMetadata())
                                    .spec(tableflowTopic.getSpec())
                                    .status(TableflowTopic.TableflowTopicStatus.builder()
                                            .id(response.id())
                                            .state(Optional.ofNullable(response.status())
                                                    .map(status -> TableflowTopic.TableflowTopicState.builder()
                                                            .phase(status.phase())
                                                            .errorMessage(
                                                                    "none".equals(status.errorMessage())
                                                                            ? null
                                                                            : status.errorMessage())
                                                            .build())
                                                    .orElse(null))
                                            .build())
                                    .build();
                        })
                        .toList());
    }

    /**
     * Validate the cluster configuration for a Tableflow topic.
     *
     * @param namespace The namespace
     * @param topic The source topic name
     * @return The validation errors
     */
    public List<String> validateConfiguration(Namespace namespace, String topic) {
        boolean configured = managedClusterProperties.stream()
                .filter(properties ->
                        properties.getName().equals(namespace.getMetadata().getCluster()))
                .anyMatch(ManagedClusterProperties::isManageTableflow);

        if (configured) {
            return List.of();
        }

        return List.of(invalidTableflowClusterConfiguration(topic));
    }

    /**
     * Validate the value schema of a Tableflow topic.
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
     * Find a Tableflow topic by namespace and name.
     *
     * @param namespace The namespace
     * @param name The source topic name
     * @return The stored Tableflow topic
     */
    public Optional<TableflowTopic> findByName(Namespace namespace, String name) {
        return tableflowTopicRepository.findByName(namespace.getMetadata().getCluster(), name);
    }

    /**
     * Create a Tableflow topic.
     *
     * @param tableflowTopic The Tableflow topic
     * @return The created Tableflow topic
     */
    public TableflowTopic create(TableflowTopic tableflowTopic) {
        return tableflowTopicRepository.create(tableflowTopic);
    }
}
