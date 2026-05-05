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
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;
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
    private final NamespaceService namespaceService;

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
            ConfluentCloudClient confluentCloudClient,
            NamespaceService namespaceService) {
        this.managedClusterProperties = managedClusterProperties;
        this.aclService = aclService;
        this.streamService = streamService;
        this.aclRepository = aclRepository;
        this.kafkaStreamRepository = kafkaStreamRepository;
        this.namespaceRepository = namespaceRepository;
        this.confluentCloudClient = confluentCloudClient;
        this.namespaceService = namespaceService;
    }

    /** Run the ACLs synchronization. */
    public void run() {
        if (!this.managedClusterProperties.isManageAcls()
                && this.managedClusterProperties.isConfluentCloud()
                && this.managedClusterProperties.isManageRbac()) {
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
            List<AccessControlEntry> aclsToDelete =
                    aclService.findNonPublicToDeleteForCluster(managedClusterProperties.getName());
            List<KafkaStream> streamsToDelete =
                    streamService.findAllToDeleteForCluster(managedClusterProperties.getName());

            createRoleBindingsFromAcls(aclsToCreate);
            createRoleBindingsFromKafkaStreams(streamsToCreate);
            deleteRoleBindingsFromAcls(aclsToDelete);
            deleteRoleBindingsFromKafkaStreams(streamsToDelete);

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
            convertAclToRoleBinding(acl).forEach(roleBinding -> confluentCloudClient
                    .createRoleBinding(managedClusterProperties.getName(), roleBinding)
                    .subscribe(
                            roleBindingResponse -> {
                                if (isUnchangedSinceLastApply(acl)) {
                                    log.info(
                                            "Success creating RoleBinding {} for ACL {} on {}",
                                            roleBindingResponse.id(),
                                            acl.getMetadata().getName(),
                                            managedClusterProperties.getName());
                                    acl.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                                    aclRepository.create(acl);
                                }
                            },
                            e -> {
                                if (isUnchangedSinceLastApply(acl)) {
                                    log.error(
                                            "Error while creating RoleBinding for ACL {} on {}",
                                            acl.getMetadata().getName(),
                                            managedClusterProperties.getName(),
                                            e);
                                    acl.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                                    aclRepository.create(acl);
                                }
                            }));
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
            confluentCloudClient
                    .createRoleBinding(managedClusterProperties.getName(), roleBinding)
                    .subscribe(
                            roleBindingResponse -> {
                                if (isUnchangedSinceLastApply(ks)) {
                                    log.info(
                                            "Success creating RoleBinding {} for KafkaStream {} on {}",
                                            roleBindingResponse.id(),
                                            ks.getMetadata().getName(),
                                            managedClusterProperties.getName());
                                    ks.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                                    kafkaStreamRepository.create(ks);
                                }
                            },
                            e -> {
                                if (isUnchangedSinceLastApply(ks)) {
                                    log.error(
                                            "Error while creating RoleBinding for KafkaStream {} on {}",
                                            ks.getMetadata().getName(),
                                            managedClusterProperties.getName(),
                                            e);
                                    ks.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                                    kafkaStreamRepository.create(ks);
                                }
                            });
        });
    }

    /**
     * Delete Role Bindings associated to Ns4Kafka ACLs.
     *
     * @param acls The Ns4Kafka ACLs
     */
    public void deleteRoleBindingsFromAcls(List<AccessControlEntry> acls) {
        // Not possible to batch delete Confluent Role Bindings
        acls.forEach(acl -> convertAclToRoleBinding(acl).forEach(roleBinding -> confluentCloudClient
                .deleteRoleBinding(managedClusterProperties.getName(), roleBinding)
                .subscribe(
                        roleBindingResponse -> {
                            if (isUnchangedSinceLastApply(acl)) {
                                log.info(
                                        "Success deleting RoleBinding {} for ACL {} on {}",
                                        roleBindingResponse.id(),
                                        acl.getMetadata().getName(),
                                        managedClusterProperties.getName());
                                aclRepository.delete(acl);
                            }
                        },
                        e -> {
                            if (isUnchangedSinceLastApply(acl)) {
                                log.error(
                                        "Error while deleting RoleBinding for ACL {} on {}",
                                        acl.getMetadata().getName(),
                                        managedClusterProperties.getName(),
                                        e);
                                acl.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                                aclRepository.create(acl);
                            }
                        })));
    }

    /**
     * Delete Role Bindings associated to Ns4Kafka Kafka Streams.
     *
     * @param kafkaStreams The Kafka Streams
     */
    public void deleteRoleBindingsFromKafkaStreams(List<KafkaStream> kafkaStreams) {
        // Not possible to batch delete Confluent Role Bindings
        kafkaStreams.forEach(ks -> {
            RoleBinding roleBindingToDelete = convertKafkaStreamToRoleBinding(ks);
            confluentCloudClient
                    .deleteRoleBinding(managedClusterProperties.getName(), roleBindingToDelete)
                    .subscribe(
                            roleBindingResponse -> {
                                if (isUnchangedSinceLastApply(ks)) {
                                    log.info(
                                            "Success deleting RoleBinding {} for KafkaStream {} on {}",
                                            roleBindingResponse.id(),
                                            ks.getMetadata().getName(),
                                            managedClusterProperties.getName());
                                    kafkaStreamRepository.delete(ks);
                                }
                            },
                            e -> {
                                if (isUnchangedSinceLastApply(ks)) {
                                    log.error(
                                            "Error while deleting RoleBinding for KafkaStream {} on {}",
                                            ks.getMetadata().getName(),
                                            managedClusterProperties.getName(),
                                            e);
                                    ks.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                                    kafkaStreamRepository.create(ks);
                                }
                            });
        });
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

        return (switch (acl.getSpec().getPermission()) {
            case OWNER, READ -> List.of(new RoleBinding(principal, DEVELOPER_READ, GROUP, computeResourcePattern(acl)));
            default ->
                throw new IllegalArgumentException(
                        "Not implemented for GROUP ACL: " + acl.getSpec().getPermission());
        });
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
     * Convert Ns4Kafka transactional ID ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    List<RoleBinding> convertTransAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = computeResourcePattern(acl);

        return (switch (acl.getSpec().getPermission()) {
            case OWNER, WRITE -> List.of(new RoleBinding(principal, DEVELOPER_WRITE, TRANSACTIONAL_ID, resource));
            default ->
                throw new IllegalArgumentException("Not implemented for TRANSACTIONAL_ID ACL: "
                        + acl.getSpec().getPermission());
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
            case TRANSACTIONAL_ID -> convertTransAclToRoleBinding(acl);
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

    /**
     * Checks whether the ACL has been reapplied since the last deployment. Avoids publishing over an ACL that has
     * already been changed.
     *
     * @param acl The ACL to deploy
     * @return True if it has been reapplied, false otherwise
     */
    private boolean isUnchangedSinceLastApply(AccessControlEntry acl) {
        Optional<AccessControlEntry> existingAcl = aclService.findByName(
                acl.getMetadata().getNamespace(), acl.getMetadata().getName());
        return existingAcl.isEmpty()
                || !acl.getMetadata()
                        .getCreationTimestamp()
                        .after(acl.getMetadata().getCreationTimestamp());
    }

    /**
     * Checks whether the Kafka stream has been reapplied since the last deployment. Avoids publishing over a Kafka
     * stream that has already been changed.
     *
     * @param kafkaStream The Kafka stream to deploy
     * @return True if it has been reapplied, false otherwise
     */
    private boolean isUnchangedSinceLastApply(KafkaStream kafkaStream) {
        Optional<Namespace> existingNamespace =
                namespaceService.findByName(kafkaStream.getMetadata().getNamespace());
        if (existingNamespace.isPresent()) {
            Optional<KafkaStream> existingStream = streamService.findByName(
                    existingNamespace.get(), kafkaStream.getMetadata().getName());
            return existingStream.isEmpty()
                    || !existingStream
                            .get()
                            .getMetadata()
                            .getCreationTimestamp()
                            .after(kafkaStream.getMetadata().getCreationTimestamp());
        }
        return true;
    }
}
