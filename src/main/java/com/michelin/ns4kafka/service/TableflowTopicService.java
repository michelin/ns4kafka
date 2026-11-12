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

import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TOPIC;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTableflowClusterConfiguration;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidTableflowValueSchema;
import static com.michelin.ns4kafka.util.enumation.Kind.TABLEFLOW_TOPIC;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TableflowTopicRepository;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import com.michelin.ns4kafka.service.client.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.util.RegexUtils;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import jakarta.inject.Singleton;
import java.util.List;
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
     * Find Tableflow topics in Confluent Cloud matching the name filter and owned by the namespace.
     *
     * @param namespace The namespace
     * @param name The name filter
     * @return The matching Tableflow topics
     */
    public Flux<TableflowTopic> findByWildcardName(Namespace namespace, String name) {
        List<String> configurationErrors = validateConfiguration(namespace, name);
        if (!configurationErrors.isEmpty()) {
            return Flux.error(new ResourceValidationException(TABLEFLOW_TOPIC, name, configurationErrors));
        }

        List<AccessControlEntry> acls = aclService.findResourceOwnerGrantedToNamespace(namespace, TOPIC);
        if (acls.isEmpty()) {
            return Flux.empty();
        }

        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return confluentCloudClient
                .listTableflowTopics(namespace.getMetadata().getCluster())
                .filter(topic ->
                        aclService.isResourceCoveredByAcls(acls, topic.spec().displayName()))
                .filter(topic ->
                        RegexUtils.isResourceCoveredByRegex(topic.spec().displayName(), nameFilterPatterns))
                .map(topic -> toTableflowTopic(namespace, topic));
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

    /**
     * Map a Confluent topic to the Ns4Kafka resource representation.
     *
     * @param namespace The namespace
     * @param response The Confluent topic
     * @return The Tableflow topic with its configuration and materialization status
     */
    private static TableflowTopic toTableflowTopic(Namespace namespace, TableflowTopicResponse response) {
        TableflowTopicResponse.Spec spec = response.spec();
        return TableflowTopic.builder()
                .metadata(Resource.Metadata.builder()
                        .name(spec.displayName())
                        .namespace(namespace.getMetadata().getName())
                        .cluster(namespace.getMetadata().getCluster())
                        .build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(spec.tableFormats())
                        .storage(TableflowTopic.Storage.builder()
                                .kind(spec.storage().kind())
                                .build())
                        .build())
                .status(response.toTopicStatus())
                .build();
    }
}
