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

import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TOPIC;
import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TRANSACTIONAL_ID;
import static com.michelin.ns4kafka.service.AclService.PUBLIC_GRANTED_TO;
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
            synchronizeConfluentRbac();
        }
    }

    /** Start the Confluent RBAC synchronization. */
    private void synchronizeConfluentRbac() {
        log.debug("Starting Role Bindings collection for cluster {}", managedClusterProperties.getName());

        try {
            // Public ACLs are handled by the ACL executor as Confluent Role Binding cannot manage "*"
            List<AccessControlEntry> aclsToCreate =
                    aclService.findAllToDeployForCluster(managedClusterProperties.getName()).stream()
                            .filter(acl ->
                                    !PUBLIC_GRANTED_TO.equals(acl.getSpec().getGrantedTo()))
                            .toList();
            List<KafkaStream> streamsToCreate =
                    streamService.findAllToDeployForCluster(managedClusterProperties.getName());

            createRbacFromAcls(aclsToCreate);
            createRbacFromKafkaStreams(streamsToCreate);

        } catch (KafkaStoreException e) {
            log.error("An error occurred collecting ACLs from Ns4kafka during Role Bindings synchronization", e);
        }
    }

    /**
     * Convert Ns4Kafka topic ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    private List<RoleBinding> convertTopicAclToRbac(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = acl.getSpec().getResource()
                + (acl.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)
                        ? "*"
                        : "");

        return (switch (acl.getSpec().getPermission()) {
            case OWNER ->
                List.of(
                        new RoleBinding(
                                USER_PRINCIPAL + principal,
                                DEVELOPER_READ,
                                AccessControlEntry.ResourceType.TOPIC,
                                resource),
                        new RoleBinding(
                                USER_PRINCIPAL + principal,
                                DEVELOPER_WRITE,
                                AccessControlEntry.ResourceType.TOPIC,
                                resource));
            case READ ->
                List.of(new RoleBinding(
                        USER_PRINCIPAL + principal, DEVELOPER_READ, AccessControlEntry.ResourceType.TOPIC, resource));
            case WRITE ->
                List.of(new RoleBinding(
                        USER_PRINCIPAL + principal, DEVELOPER_WRITE, AccessControlEntry.ResourceType.TOPIC, resource));
        });
    }

    /**
     * Convert Ns4Kafka group ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    private List<RoleBinding> convertGroupAclToRbac(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = acl.getSpec().getResource()
                + (acl.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)
                        ? "*"
                        : "");

        List<RoleBinding> results = new ArrayList<>();

        switch (acl.getSpec().getPermission()) {
            case OWNER, READ:
                results.add(new RoleBinding(
                        USER_PRINCIPAL + principal, DEVELOPER_READ, AccessControlEntry.ResourceType.GROUP, resource));
                break;
            default:
                throw new IllegalArgumentException(
                        "Not implemented for GROUP ACL: " + acl.getSpec().getPermission());
        }

        if (namespace.getSpec().isTransactionsEnabled()) {
            // EOS connectors need "write" on "connect-cluster-${groupId}".
            results.add(
                    new RoleBinding(principal, DEVELOPER_WRITE, TRANSACTIONAL_ID, "connect-cluster-" + resource + "*"));

            // EOS connectors need "write" on "${groupId}-${connector}-${taskId}".
            // PREFIXED ACLs to cover all Kafka Streams & EOS connectors.
            results.add(new RoleBinding(principal, DEVELOPER_WRITE, TRANSACTIONAL_ID, resource + "*"));
        } else if (streamService.hasKafkaStream(namespace)) {
            results.add(new RoleBinding(
                    principal, DEVELOPER_WRITE, AccessControlEntry.ResourceType.TRANSACTIONAL_ID, resource + "*"));
        }

        return results;
    }

    /**
     * Convert Ns4Kafka connect ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    private List<RoleBinding> convertConnectAclToRbac(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = "connect-" + acl.getSpec().getResource()
                + (acl.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)
                        ? "*"
                        : "");

        return (switch (acl.getSpec().getPermission()) {
            case OWNER ->
                List.of(new RoleBinding(
                        USER_PRINCIPAL + principal, DEVELOPER_READ, AccessControlEntry.ResourceType.TOPIC, resource));
            default -> List.of();
        });
    }

    /**
     * Convert Ns4Kafka ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    private List<RoleBinding> convertAclToRbac(AccessControlEntry acl) {
        return switch (acl.getSpec().getResourceType()) {
            case TOPIC -> convertTopicAclToRbac(acl);
            case GROUP -> convertGroupAclToRbac(acl);
            case CONNECT -> convertConnectAclToRbac(acl);
            default -> List.of();
        };
    }

    /**
     * Convert Kafka Stream into Role Binding.
     *
     * @param principal The Kafka principal
     * @param stream The Kafka Stream resource
     * @return A Role Binding
     */
    private RoleBinding convertKafkaStreamToRbac(String principal, KafkaStream stream) {
        return new RoleBinding(
                principal, DEVELOPER_MANAGE, TOPIC, stream.getMetadata().getName() + "*");
    }

    /**
     * Create RBACs from a list of ACLs.
     *
     * @param toCreate The list of ACLs to create as RBAC
     */
    private void createRbacFromAcls(List<AccessControlEntry> toCreate) {
        // Currently no possible to batch create Confluent RBAC
        toCreate.forEach(acl -> {
            List<RoleBinding> roleBindings = convertAclToRbac(acl);

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
     * Create RBACs from a list of KafkaStreams.
     *
     * @param toCreate The list of KafkaStreams to create as RBAC
     */
    private void createRbacFromKafkaStreams(List<KafkaStream> toCreate) {
        // Currently no possible to batch create Confluent RBAC
        toCreate.forEach(ks -> {
            Namespace namespace = namespaceRepository
                    .findByName(ks.getMetadata().getNamespace())
                    .orElseThrow();
            String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
            RoleBinding roleBinding = convertKafkaStreamToRbac(principal, ks);

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
     * Delete a RBAC.
     *
     * @param acl The Ns4Kafka ACL to delete
     */
    public void deleteRbac(AccessControlEntry acl) {
        // Currently no possible to batch delete Confluent RBAC
        convertAclToRbac(acl).forEach(roleBinding -> {
            try {
                confluentCloudClient.deleteRoleBinding(managedClusterProperties.getName(), roleBinding);
                log.info(
                        "Success deleting RBAC {} on {}",
                        acl.getMetadata().getName(),
                        managedClusterProperties.getName());
            } catch (Exception e) {
                log.error(
                        "Error while deleting RBAC {} on {}",
                        acl.getMetadata().getName(),
                        managedClusterProperties.getName(),
                        e);
            }
        });
    }

    /**
     * Delete a given Kafka Streams.
     *
     * @param kafkaStream The Kafka Streams
     */
    public void deleteKafkaStreams(Namespace namespace, KafkaStream kafkaStream) {
        if (managedClusterProperties.isManageRbac()) {
            String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
            RoleBinding roleBindingToDelete = convertKafkaStreamToRbac(principal, kafkaStream);
            confluentCloudClient.deleteRoleBinding(managedClusterProperties.getName(), roleBindingToDelete);
        }
    }
}
