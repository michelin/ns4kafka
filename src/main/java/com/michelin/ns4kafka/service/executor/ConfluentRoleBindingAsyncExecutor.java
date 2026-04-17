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
package com.michelin.ns4kafka.service.executor;

import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.GROUP;
import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TOPIC;
import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TRANSACTIONAL_ID;
import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_MANAGE;
import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_READ;
import static com.michelin.ns4kafka.util.enumation.ConfluentRole.DEVELOPER_WRITE;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.repository.kafka.KafkaStreamRepository;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Access control entry executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class ConfluentRoleBindingAsyncExecutor {
    private static final String USER_PRINCIPAL = "User:";

    private final ManagedClusterProperties managedClusterProperties;
    private final AclService aclService;
    private final StreamService streamService;
    private final NamespaceRepository namespaceRepository;
    private final AccessControlEntryRepository aclRepository;
    private final KafkaStreamRepository kafkaStreamRepository;
    private final ConfluentCloudClient confluentCloudClient;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param aclService The ACL service
     * @param streamService The stream service
     * @param namespaceRepository The namespace repository
     */
    public ConfluentRoleBindingAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            AclService aclService,
            StreamService streamService,
            NamespaceRepository namespaceRepository,
            AccessControlEntryRepository aclRepository,
            KafkaStreamRepository kafkaStreamRepository,
            ConfluentCloudClient confluentCloudClient) {
        this.managedClusterProperties = managedClusterProperties;
        this.aclService = aclService;
        this.streamService = streamService;
        this.aclRepository = aclRepository;
        this.kafkaStreamRepository = kafkaStreamRepository;
        this.namespaceRepository = namespaceRepository;
        this.confluentCloudClient = confluentCloudClient;
    }

    /** Run the ACLs synchronization. */
    public void run() {
        if (this.managedClusterProperties.isConfluentCloud() && this.managedClusterProperties.isManageRbac()) {
            synchronizeConfluentRoleBindings();
        }
    }

    /** Start the Confluent Role Bindings synchronization. */
    void synchronizeConfluentRoleBindings() {
        log.debug("Starting Role Bindings collection for cluster {}", managedClusterProperties.getName());

        try {
            // Public ACLs are handled by the ACL executor as Confluent Role Binding cannot manage "*"
            List<AccessControlEntry> aclsToCreate =
                    aclService.findNonPublicToDeployForCluster(managedClusterProperties.getName());
            List<KafkaStream> streamsToCreate =
                    streamService.findAllToDeployForCluster(managedClusterProperties.getName());

            createRoleBindingsFromAcls(aclsToCreate);
            createRoleBindingsFromKafkaStreams(streamsToCreate);

        } catch (KafkaStoreException e) {
            log.error("An error occurred collecting ACLs from Ns4kafka during Role Bindings synchronization", e);
        }
    }

    /**
     * Create Role Bindings from ACLs.
     *
     * @param toCreate The list of ACLs
     */
    void createRoleBindingsFromAcls(List<AccessControlEntry> toCreate) {
        // Currently no possible to batch create Confluent Role Bindings
        toCreate.forEach(acl -> {
            List<RoleBinding> roleBindings = convertAclToRoleBinding(acl);

            roleBindings.forEach(roleBinding -> {
                try {
                    confluentCloudClient.createRoleBinding(managedClusterProperties.getName(), roleBinding);
                    acl.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                    aclRepository.create(acl);

                    log.info(
                            "Success creating ACL RoleBinding {} on {}",
                            acl.getMetadata().getName(),
                            managedClusterProperties.getName());
                } catch (Exception e) {
                    log.error(
                            "Error while creating ACL RoleBinding {} on {}",
                            acl.getMetadata().getName(),
                            managedClusterProperties.getName(),
                            e);
                }
            });
        });
    }

    /**
     * Create Role Bindings from Kafka Streams.
     *
     * @param toCreate The list of Kafka Streams
     */
    void createRoleBindingsFromKafkaStreams(List<KafkaStream> toCreate) {
        // Currently no possible to batch create Confluent Role Bindings
        toCreate.forEach(ks -> {
            RoleBinding roleBinding = convertKafkaStreamToRoleBinding(ks);

            try {
                confluentCloudClient.createRoleBinding(managedClusterProperties.getName(), roleBinding);
                ks.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                kafkaStreamRepository.create(ks);
                log.info(
                        "Success creating KafkaStream RoleBinding {} on {}",
                        ks.getMetadata().getName(),
                        managedClusterProperties.getName());
            } catch (Exception e) {
                log.error(
                        "Error while creating KafkaStream RoleBinding {} on {}",
                        ks.getMetadata().getName(),
                        managedClusterProperties.getName(),
                        e);
            }
        });
    }

    /**
     * Delete Role Bindings associated to a Ns4Kafka ACL.
     *
     * @param acl The Ns4Kafka ACL
     */
    public void deleteRoleBindingsFromAcl(AccessControlEntry acl) {
        if (managedClusterProperties.isManageRbac()) {
            // Currently no possible to batch delete Confluent Role Bindings
            convertAclToRoleBinding(acl).forEach(roleBinding -> {
                try {
                    confluentCloudClient.deleteRoleBinding(managedClusterProperties.getName(), roleBinding);
                    log.info(
                            "Success deleting ACL RoleBinding {} on {}",
                            acl.getMetadata().getName(),
                            managedClusterProperties.getName());
                } catch (Exception e) {
                    log.error(
                            "Error while deleting ACL RoleBinding {} on {}",
                            acl.getMetadata().getName(),
                            managedClusterProperties.getName(),
                            e);
                }
            });
        }
    }

    /**
     * Delete Role Bindings associated to a Ns4Kafka Kafka Stream.
     *
     * @param kafkaStream The Kafka Streams
     */
    public void deleteRoleBindingFromKafkaStream(KafkaStream kafkaStream) {
        if (managedClusterProperties.isManageRbac()) {
            RoleBinding roleBindingToDelete = convertKafkaStreamToRoleBinding(kafkaStream);
            try {
                confluentCloudClient.deleteRoleBinding(managedClusterProperties.getName(), roleBindingToDelete);
                log.info(
                        "Success deleting KafkaStream RoleBinding {} on {}",
                        kafkaStream.getMetadata().getName(),
                        managedClusterProperties.getName());
            } catch (Exception e) {
                log.error(
                        "Error while deleting KafkaStream RoleBinding {} on {}",
                        kafkaStream.getMetadata().getName(),
                        managedClusterProperties.getName(),
                        e);
            }
        }
    }

    /**
     * Compute pattern from ACL.
     *
     * @param acl The Ns4Kafka ACL
     * @return The resource pattern string
     */
    String computeResourcePattern(AccessControlEntry acl) {
        return acl.getSpec().getResource()
                + (AccessControlEntry.ResourcePatternType.PREFIXED.equals(
                                acl.getSpec().getResourcePatternType())
                        ? "*"
                        : "");
    }

    /**
     * Convert Ns4Kafka topic ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    List<RoleBinding> convertTopicAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = computeResourcePattern(acl);

        return (switch (acl.getSpec().getPermission()) {
            case OWNER ->
                List.of(
                        new RoleBinding(principal, DEVELOPER_READ, TOPIC, resource),
                        new RoleBinding(principal, DEVELOPER_WRITE, TOPIC, resource));
            case READ -> List.of(new RoleBinding(principal, DEVELOPER_READ, TOPIC, resource));
            case WRITE -> List.of(new RoleBinding(principal, DEVELOPER_WRITE, TOPIC, resource));
        });
    }

    /**
     * Convert Ns4Kafka group ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    List<RoleBinding> convertGroupAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = acl.getSpec().getResource() + "*";
        List<RoleBinding> results = new ArrayList<>();

        switch (acl.getSpec().getPermission()) {
            case OWNER, READ:
                results.add(new RoleBinding(principal, DEVELOPER_READ, GROUP, computeResourcePattern(acl)));
                break;
            default:
                throw new IllegalArgumentException(
                        "Not implemented for GROUP ACL: " + acl.getSpec().getPermission());
        }

        if (namespace.getSpec().isTransactionsEnabled()) {
            // EOS connectors need "write" on "connect-cluster-${groupId}".
            results.add(new RoleBinding(principal, DEVELOPER_WRITE, TRANSACTIONAL_ID, "connect-cluster-" + resource));

            // EOS connectors need "write" on "${groupId}-${connector}-${taskId}".
            // PREFIXED ACLs to cover all Kafka Streams & EOS connectors.
            results.add(new RoleBinding(principal, DEVELOPER_WRITE, TRANSACTIONAL_ID, resource));
        } else if (streamService.hasKafkaStream(namespace)) {
            results.add(new RoleBinding(principal, DEVELOPER_WRITE, TRANSACTIONAL_ID, resource));
        }

        return results;
    }

    /**
     * Convert Ns4Kafka connect ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    List<RoleBinding> convertConnectAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = "connect-" + computeResourcePattern(acl);

        return (switch (acl.getSpec().getPermission()) {
            case OWNER -> List.of(new RoleBinding(principal, DEVELOPER_READ, GROUP, resource));
            default ->
                throw new IllegalArgumentException(
                        "Not implemented for CONNECT ACL: " + acl.getSpec().getPermission());
        });
    }

    /**
     * Convert Ns4Kafka ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    List<RoleBinding> convertAclToRoleBinding(AccessControlEntry acl) {
        return switch (acl.getSpec().getResourceType()) {
            case TOPIC -> convertTopicAclToRoleBinding(acl);
            case GROUP -> convertGroupAclToRoleBinding(acl);
            case CONNECT -> convertConnectAclToRoleBinding(acl);
            default -> List.of();
        };
    }

    /**
     * Convert Kafka Stream into Role Binding.
     *
     * @param stream The Kafka Stream resource
     * @return A Role Binding
     */
    RoleBinding convertKafkaStreamToRoleBinding(KafkaStream stream) {
        Namespace namespace = namespaceRepository
                .findByName(stream.getMetadata().getNamespace())
                .orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();

        return new RoleBinding(
                principal, DEVELOPER_MANAGE, TOPIC, stream.getMetadata().getName() + "*");
    }
}
