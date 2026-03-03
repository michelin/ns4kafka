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

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNamespaceNoCluster;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNamespaceUserAlreadyExist;
import static com.michelin.ns4kafka.util.enumation.Kind.ACCESS_CONTROL_ENTRY;
import static com.michelin.ns4kafka.util.enumation.Kind.CONNECTOR;
import static com.michelin.ns4kafka.util.enumation.Kind.CONNECT_CLUSTER;
import static com.michelin.ns4kafka.util.enumation.Kind.RESOURCE_QUOTA;
import static com.michelin.ns4kafka.util.enumation.Kind.ROLE_BINDING;
import static com.michelin.ns4kafka.util.enumation.Kind.TOPIC;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.util.FormatErrorUtils;
import com.michelin.ns4kafka.util.RegexUtils;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.kafka.common.config.TopicConfig;

/** Service to manage the namespaces. */
@Singleton
public class NamespaceService {
    private static final List<String> NOT_EDITABLE_CONFIGS = List.of(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG);

    private final TopicService topicService;
    private final RoleBindingService roleBindingService;
    private final AclService aclService;
    private final ConnectorService connectorService;
    private final ConnectClusterService connectClusterService;
    private final ResourceQuotaService resourceQuotaService;
    private final NamespaceRepository namespaceRepository;
    private final List<ManagedClusterProperties> managedClusterProperties;

    /**
     * Constructor.
     *
     * @param topicService The topic service
     * @param roleBindingService The role binding service
     * @param aclService The ACL service
     * @param connectorService The connector service
     * @param connectClusterService The Connect cluster service
     * @param resourceQuotaService The resource quota service
     * @param namespaceRepository The namespace repository
     * @param managedClusterProperties The managed cluster properties
     */
    public NamespaceService(
            TopicService topicService,
            RoleBindingService roleBindingService,
            AclService aclService,
            ConnectorService connectorService,
            ConnectClusterService connectClusterService,
            ResourceQuotaService resourceQuotaService,
            NamespaceRepository namespaceRepository,
            List<ManagedClusterProperties> managedClusterProperties) {
        this.topicService = topicService;
        this.roleBindingService = roleBindingService;
        this.aclService = aclService;
        this.connectorService = connectorService;
        this.connectClusterService = connectClusterService;
        this.resourceQuotaService = resourceQuotaService;
        this.namespaceRepository = namespaceRepository;
        this.managedClusterProperties = managedClusterProperties;
    }

    /**
     * List all namespaces.
     *
     * @return The list of namespaces
     */
    public List<Namespace> findAll() {
        return managedClusterProperties.stream()
                .map(ManagedClusterProperties::getName)
                .flatMap(s -> namespaceRepository.findAllForCluster(s).stream())
                .toList();
    }

    /**
     * List all namespaces, filtered by name parameter.
     *
     * @param name The name filter
     * @return The list of namespaces
     */
    public List<Namespace> findByWildcardName(String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAll().stream()
                .filter(ns ->
                        RegexUtils.isResourceCoveredByRegex(ns.getMetadata().getName(), nameFilterPatterns))
                .toList();
    }

    /**
     * Find the namespace which is owner of the given topic name, out of the given list.
     *
     * @param namespaces The namespaces list
     * @param topic The topic name to search
     * @return The namespace which is owner of the given topic name
     */
    public Optional<Namespace> findByTopicName(List<Namespace> namespaces, String topic) {
        return namespaces.stream()
                .filter(ns -> aclService.isResourceCoveredByAcls(
                        aclService.findResourceOwnerGrantedToNamespace(ns, AccessControlEntry.ResourceType.TOPIC),
                        topic))
                .findFirst();
    }

    /**
     * Find a namespace by name.
     *
     * @param namespace The namespace
     * @return An optional namespace
     */
    public Optional<Namespace> findByName(String namespace) {
        return namespaceRepository.findByName(namespace);
    }

    /**
     * Validate new namespace creation.
     *
     * @param namespace The namespace to create
     * @return A list of validation errors
     */
    public List<String> validateCreation(Namespace namespace) {
        List<String> validationErrors = new ArrayList<>();

        if (managedClusterProperties.stream().noneMatch(config -> config.getName()
                .equals(namespace.getMetadata().getCluster()))) {
            validationErrors.add(
                    invalidNamespaceNoCluster(namespace.getMetadata().getCluster()));
        }

        return validationErrors;
    }

    /**
     * Validate the Connect clusters of the namespace.
     *
     * @param namespace The namespace
     * @return A list of validation errors
     */
    public List<String> validate(Namespace namespace) {
        List<String> validationErrors = new ArrayList<>();

        Optional<ManagedClusterProperties> managedCluster = managedClusterProperties.stream()
                .filter(cluster -> namespace.getMetadata().getCluster().equals(cluster.getName()))
                .findFirst();

        if (managedCluster.isPresent()) {
            if (managedCluster.get().isConfluentCloud()) {
                validationErrors.addAll(NOT_EDITABLE_CONFIGS.stream()
                        .filter(config -> namespace
                                .getSpec()
                                .getTopicValidator()
                                .getValidationConstraints()
                                .containsKey(config))
                        .map(FormatErrorUtils::invalidNamespaceTopicValidatorKeyConfluentCloud)
                        .toList());
            }

            validationErrors.addAll(namespace.getSpec().getConnectClusters().stream()
                    .filter(connectCluster ->
                            !managedCluster.get().getConnects().containsKey(connectCluster))
                    .map(FormatErrorUtils::invalidNamespaceNoConnectCluster)
                    .toList());
        }

        if (namespaceRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
                .filter(foundNamespace -> !foundNamespace
                        .getMetadata()
                        .getName()
                        .equals(namespace.getMetadata().getName()))
                .anyMatch(foundNamespace -> foundNamespace
                        .getSpec()
                        .getKafkaUser()
                        .equals(namespace.getSpec().getKafkaUser()))) {
            validationErrors.add(
                    invalidNamespaceUserAlreadyExist(namespace.getSpec().getKafkaUser()));
        }

        return validationErrors;
    }

    /**
     * Create or update a namespace.
     *
     * @param namespace The namespace to create or update
     * @return The created or updated namespace
     */
    public Namespace createOrUpdate(Namespace namespace) {
        return namespaceRepository.createNamespace(namespace);
    }

    /**
     * Delete a namespace.
     *
     * @param namespace The namespace to delete
     */
    public void delete(Namespace namespace) {
        aclService.deleteAllGrantedToNamespace(namespace);
        namespaceRepository.delete(namespace);
    }

    /**
     * List all resources of a namespace.
     *
     * @param namespace The namespace
     * @return The list of resources
     */
    public List<String> findAllResourcesByNamespace(Namespace namespace) {
        return Stream.of(
                        topicService.findAllForNamespace(namespace).stream()
                                .map(topic -> TOPIC + "/" + topic.getMetadata().getName()),
                        connectorService.findAllForNamespace(namespace).stream()
                                .map(connector -> CONNECTOR + "/"
                                        + connector.getMetadata().getName()),
                        connectClusterService.findAllForNamespaceWithOwnerPermission(namespace).stream()
                                .map(connectCluster -> CONNECT_CLUSTER + "/"
                                        + connectCluster.getMetadata().getName()),
                        aclService.findAllForNamespace(namespace).stream()
                                .map(ace -> ACCESS_CONTROL_ENTRY + "/"
                                        + ace.getMetadata().getName()),
                        resourceQuotaService
                                .findForNamespace(namespace.getMetadata().getName())
                                .stream()
                                .map(resourceQuota -> RESOURCE_QUOTA + "/"
                                        + resourceQuota.getMetadata().getName()),
                        roleBindingService
                                .findAllForNamespace(namespace.getMetadata().getName())
                                .stream()
                                .map(roleBinding -> ROLE_BINDING + "/"
                                        + roleBinding.getMetadata().getName()))
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
                .toList();
    }
}
