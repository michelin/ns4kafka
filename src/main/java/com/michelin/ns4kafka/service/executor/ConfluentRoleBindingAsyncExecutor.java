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
    private final ConfluentCloudClient confluentCloudClient;
    private final AclService aclService;
    private final NamespaceService namespaceService;
    private final StreamService streamService;
    private final AccessControlEntryRepository aclRepository;
    private final KafkaStreamRepository kafkaStreamRepository;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param confluentCloudClient The Confluent Cloud client
     * @param aclService The ACL service
     * @param namespaceService The namespace service
     * @param streamService The stream service
     * @param aclRepository The ACL repository
     * @param kafkaStreamRepository The Kafka Stream repository
     */
    public ConfluentRoleBindingAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            ConfluentCloudClient confluentCloudClient,
            AclService aclService,
            NamespaceService namespaceService,
            StreamService streamService,
            AccessControlEntryRepository aclRepository,
            KafkaStreamRepository kafkaStreamRepository) {
        this.managedClusterProperties = managedClusterProperties;
        this.confluentCloudClient = confluentCloudClient;
        this.aclService = aclService;
        this.namespaceService = namespaceService;
        this.streamService = streamService;
        this.aclRepository = aclRepository;
        this.kafkaStreamRepository = kafkaStreamRepository;
    }

    /** Run the ACLs synchronization. */
    public void run() {
        if (!this.managedClusterProperties.isManageAcls()
                && this.managedClusterProperties.isConfluentCloud()
                && this.managedClusterProperties.isManageRbac()) {
            log.debug("Starting Role Bindings collection for cluster {}", managedClusterProperties.getName());

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
        }
    }

    /**
     * Create Role Bindings from ACLs.
     *
     * @param toCreate The list of ACLs
     */
    void createRoleBindingsFromAcls(List<AccessControlEntry> toCreate) {
        // Currently no possible to batch create Confluent Role Bindings
        toCreate.forEach(acl -> convertAclToRoleBinding(acl)
                .forEach(roleBinding -> confluentCloudClient
                        .createRoleBinding(managedClusterProperties.getName(), roleBinding)
                        .subscribe(
                                roleBindingResponse -> {
                                    Optional<AccessControlEntry> existingAcl = aclService.findByName(
                                            acl.getMetadata().getNamespace(),
                                            acl.getMetadata().getName());

                                    AccessControlEntry lastVersion = existingAcl.orElse(acl);
                                    lastVersion
                                            .getMetadata()
                                            .setGeneration(
                                                    lastVersion.getMetadata().getGeneration() + 1);

                                    // Only mark ACL as success if it has not been re-applied since last deployment
                                    boolean unchangedSinceLastApply = existingAcl.isEmpty()
                                            || !existingAcl
                                                    .get()
                                                    .getMetadata()
                                                    .getUpdateTimestamp()
                                                    .after(acl.getMetadata().getUpdateTimestamp());
                                    if (unchangedSinceLastApply) {
                                        lastVersion.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                                    }

                                    aclRepository.create(lastVersion);

                                    log.info(
                                            "Success creating RoleBinding {} for ACL {} on {}.",
                                            roleBindingResponse.id(),
                                            lastVersion.getMetadata().getName(),
                                            managedClusterProperties.getName());
                                },
                                e -> {
                                    if (isUnchangedSinceLastApply(acl)) {
                                        log.error(
                                                "Error creating RoleBinding for ACL {} on {}.",
                                                acl.getMetadata().getName(),
                                                managedClusterProperties.getName(),
                                                e);

                                        acl.getMetadata()
                                                .setStatus(Resource.Metadata.Status.ofCreationFailed(e.getMessage()));
                                        aclRepository.create(acl);
                                    }
                                })));
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
                                Optional<KafkaStream> existingStream = namespaceService
                                        .findByName(ks.getMetadata().getNamespace())
                                        .flatMap(namespace -> streamService.findByName(
                                                namespace, ks.getMetadata().getName()));

                                KafkaStream lastVersion = existingStream.orElse(ks);
                                lastVersion
                                        .getMetadata()
                                        .setGeneration(lastVersion.getMetadata().getGeneration() + 1);

                                // Only mark Kafka stream as success if it has not been re-applied since last deployment
                                boolean unchangedSinceLastApply = existingStream.isEmpty()
                                        || !existingStream
                                                .get()
                                                .getMetadata()
                                                .getUpdateTimestamp()
                                                .after(ks.getMetadata().getUpdateTimestamp());
                                if (unchangedSinceLastApply) {
                                    lastVersion.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                                }

                                kafkaStreamRepository.create(lastVersion);

                                log.info(
                                        "Success creating RoleBinding {} for KafkaStream {} on {}.",
                                        roleBindingResponse.id(),
                                        lastVersion.getMetadata().getName(),
                                        managedClusterProperties.getName());
                            },
                            e -> {
                                if (isUnchangedSinceLastApply(ks)) {
                                    log.error(
                                            "Error creating RoleBinding for KafkaStream {} on {}.",
                                            ks.getMetadata().getName(),
                                            managedClusterProperties.getName(),
                                            e);

                                    ks.getMetadata()
                                            .setStatus(Resource.Metadata.Status.ofCreationFailed(e.getMessage()));
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
        acls.forEach(acl -> convertAclToRoleBinding(acl)
                .forEach(roleBinding -> confluentCloudClient
                        .deleteRoleBinding(managedClusterProperties.getName(), roleBinding)
                        .doOnSuccess(roleBindingResponse -> {
                            if (roleBindingResponse == null) {
                                log.info(
                                        "No RoleBinding to delete for ACL {} on {}: ACL will be removed from Ns4Kafka.",
                                        acl.getMetadata().getName(),
                                        managedClusterProperties.getName());

                                aclRepository.delete(acl);
                            } else if (isUnchangedSinceLastApply(acl)) {
                                log.info(
                                        "Success deleting RoleBinding {} for ACL {} on {}.",
                                        roleBindingResponse.id(),
                                        acl.getMetadata().getName(),
                                        managedClusterProperties.getName());

                                aclRepository.delete(acl);
                            }
                        })
                        .doOnError(e -> {
                            if (isUnchangedSinceLastApply(acl)) {
                                log.error(
                                        "Error deleting RoleBinding for ACL {} on {}.",
                                        acl.getMetadata().getName(),
                                        managedClusterProperties.getName(),
                                        e);

                                acl.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                                aclRepository.create(acl);
                            }
                        })
                        .subscribe()));
    }

    /**
     * Delete Role Bindings associated to Ns4Kafka Kafka Streams.
     *
     * @param kafkaStreams The Kafka Streams
     */
    public void deleteRoleBindingsFromKafkaStreams(List<KafkaStream> kafkaStreams) {
        // Not possible to batch delete Confluent Role Bindings
        kafkaStreams.forEach(ks -> confluentCloudClient
                .deleteRoleBinding(managedClusterProperties.getName(), convertKafkaStreamToRoleBinding(ks))
                .doOnSuccess(roleBindingResponse -> {
                    if (roleBindingResponse == null) {
                        log.info(
                                "No RoleBinding to delete for KafkaStream {} on {}: KafkaStream will be removed from Ns4Kafka.",
                                ks.getMetadata().getName(),
                                managedClusterProperties.getName());

                        kafkaStreamRepository.delete(ks);
                    } else if (isUnchangedSinceLastApply(ks)) {
                        log.info(
                                "Success deleting RoleBinding {} for KafkaStream {} on {}.",
                                roleBindingResponse.id(),
                                ks.getMetadata().getName(),
                                managedClusterProperties.getName());

                        kafkaStreamRepository.delete(ks);
                    }
                })
                .doOnError(e -> {
                    if (isUnchangedSinceLastApply(ks)) {
                        log.error(
                                "Error deleting RoleBinding for KafkaStream {} on {}",
                                ks.getMetadata().getName(),
                                managedClusterProperties.getName(),
                                e);

                        ks.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                        kafkaStreamRepository.create(ks);
                    }
                })
                .subscribe());
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
                namespaceService.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = computeResourcePattern(acl);

        return switch (acl.getSpec().getPermission()) {
            case OWNER ->
                List.of(
                        new RoleBinding(principal, DEVELOPER_READ, TOPIC, resource),
                        new RoleBinding(principal, DEVELOPER_WRITE, TOPIC, resource));
            case READ -> List.of(new RoleBinding(principal, DEVELOPER_READ, TOPIC, resource));
            case WRITE -> List.of(new RoleBinding(principal, DEVELOPER_WRITE, TOPIC, resource));
        };
    }

    /**
     * Convert Ns4Kafka group ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    List<RoleBinding> convertGroupAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceService.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();

        return switch (acl.getSpec().getPermission()) {
            case OWNER, READ -> List.of(new RoleBinding(principal, DEVELOPER_READ, GROUP, computeResourcePattern(acl)));
            default ->
                throw new IllegalArgumentException(
                        "Not implemented for GROUP ACL: " + acl.getSpec().getPermission());
        };
    }

    /**
     * Convert Ns4Kafka connect ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    List<RoleBinding> convertConnectAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceService.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = "connect-" + computeResourcePattern(acl);

        if (acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
            return List.of(new RoleBinding(principal, DEVELOPER_READ, GROUP, resource));
        }

        throw new IllegalArgumentException(
                "Not implemented for CONNECT ACL: " + acl.getSpec().getPermission());
    }

    /**
     * Convert Ns4Kafka transactional ID ACL into Role Binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Role Bindings
     */
    List<RoleBinding> convertTransAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceService.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = computeResourcePattern(acl);

        return switch (acl.getSpec().getPermission()) {
            case OWNER, WRITE -> List.of(new RoleBinding(principal, DEVELOPER_WRITE, TRANSACTIONAL_ID, resource));
            default ->
                throw new IllegalArgumentException("Not implemented for TRANSACTIONAL_ID ACL: "
                        + acl.getSpec().getPermission());
        };
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
        Namespace namespace =
                namespaceService.findByName(stream.getMetadata().getNamespace()).orElseThrow();
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
                || !existingAcl
                        .get()
                        .getMetadata()
                        .getUpdateTimestamp()
                        .after(acl.getMetadata().getUpdateTimestamp());
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
                            .getUpdateTimestamp()
                            .after(kafkaStream.getMetadata().getUpdateTimestamp());
        }
        return true;
    }
}
