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

import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.CONNECT;

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
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/** Access control entry executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class AccessControlEntryAsyncExecutor {
    private static final String USER_PRINCIPAL = "User:";
    private static final String USER_PRINCIPAL_PUBLIC = "User:*";
    private static final String ERROR = "Error";

    private final ManagedClusterProperties managedClusterProperties;
    private final AclService aclService;
    private final StreamService streamService;
    private final NamespaceService namespaceService;
    private final AccessControlEntryRepository aclRepository;
    private final KafkaStreamRepository kafkaStreamRepository;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param aclService The ACL service
     * @param streamService The stream service
     * @param namespaceService The namespace service
     * @param aclRepository The ACL repository
     * @param kafkaStreamRepository The Kafka Stream repository
     */
    public AccessControlEntryAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            AclService aclService,
            StreamService streamService,
            NamespaceService namespaceService,
            AccessControlEntryRepository aclRepository,
            KafkaStreamRepository kafkaStreamRepository) {
        this.managedClusterProperties = managedClusterProperties;
        this.aclService = aclService;
        this.streamService = streamService;
        this.namespaceService = namespaceService;
        this.aclRepository = aclRepository;
        this.kafkaStreamRepository = kafkaStreamRepository;
    }

    /** Run the ACLs synchronization. */
    public void run() {
        if (this.managedClusterProperties.isManageAcls()) {
            synchronizeAcls();
        }
        if (this.managedClusterProperties.isManageRbac()) {
            synchronizePublicAcls();
        }
    }

    /** Start the ACLs synchronization. */
    private void synchronizeAcls() {
        log.debug("Starting ACL deployment for cluster {}", managedClusterProperties.getName());

        try {
            // When Confluent Role Bindings activated, only handle public Kafka ACLs
            List<AccessControlEntry> aclsToCreate =
                    aclService.findAllToDeployForCluster(managedClusterProperties.getName());
            List<AccessControlEntry> aclsToDelete =
                    aclService.findAllToDeleteForCluster(managedClusterProperties.getName());
            List<KafkaStream> streamsToCreate =
                    streamService.findAllToDeployForCluster(managedClusterProperties.getName());
            List<KafkaStream> streamsToDelete =
                    streamService.findAllToDeleteForCluster(managedClusterProperties.getName());

            createAcls(aclsToCreate);
            createAclsFromKafkaStreams(streamsToCreate);
            deleteAcls(aclsToDelete);
            deleteAclsFromKafkaStreams(streamsToDelete);

        } catch (Exception e) {
            log.error("An error occurred creating or deleting ACLs during ACLs synchronization", e);
        }
    }

    /** Start the ACLs synchronization. */
    private void synchronizePublicAcls() {
        log.debug("Starting public ACL deployment for cluster {}", managedClusterProperties.getName());

        try {
            List<AccessControlEntry> aclsToCreate =
                    aclService.findPublicToDeleteForCluster(managedClusterProperties.getName());
            List<AccessControlEntry> aclsToDelete =
                    aclService.findPublicToDeployForCluster(managedClusterProperties.getName());

            createAcls(aclsToCreate);
            deleteAcls(aclsToDelete);
        } catch (Exception e) {
            log.error("An error occurred creating or deleting public ACLs during ACLs synchronization", e);
        }
    }
    /**
     * Convert Ns4Kafka ACL into Kafka ACLs.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Kafka ACLs
     */
    List<AclBinding> convertAclToAclBindings(AccessControlEntry acl) {
        String connectPrefix = CONNECT == acl.getSpec().getResourceType() ? "connect-" : "";

        // Convert pattern & resource type from Ns4Kafka to org.apache.kafka.common types
        PatternType patternType =
                PatternType.fromString(acl.getSpec().getResourcePatternType().toString());
        ResourceType resourceType = CONNECT == acl.getSpec().getResourceType()
                ? ResourceType.GROUP
                : ResourceType.fromString(acl.getSpec().getResourceType().toString());
        ResourcePattern resourcePattern =
                new ResourcePattern(resourceType, connectPrefix + acl.getSpec().getResource(), patternType);

        Set<AclOperation> aclOperations =
                switch (acl.getSpec().getResourceType()) {
                    case TOPIC ->
                        acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER
                                ? EnumSet.of(AclOperation.WRITE, AclOperation.READ, AclOperation.DESCRIBE_CONFIGS)
                                : EnumSet.of(AclOperation.fromString(
                                        acl.getSpec().getPermission().toString()));
                    case GROUP, CONNECT ->
                        acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER
                                ? EnumSet.of(AclOperation.READ)
                                : EnumSet.of(AclOperation.fromString(
                                        acl.getSpec().getPermission().toString()));
                    case TRANSACTIONAL_ID -> EnumSet.of(AclOperation.WRITE, AclOperation.DESCRIBE_CONFIGS);
                    default -> Set.of();
                };

        if (aclService.isPublicAcl(acl)) {
            return aclOperations.stream()
                    .map(aclOperation -> new AclBinding(
                            resourcePattern,
                            new org.apache.kafka.common.acl.AccessControlEntry(
                                    USER_PRINCIPAL_PUBLIC, "*", aclOperation, AclPermissionType.ALLOW)))
                    .toList();
        }

        Namespace namespace =
                namespaceService.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();

        return aclOperations.stream()
                .map(aclOperation -> new AclBinding(
                        resourcePattern,
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                principal, "*", aclOperation, AclPermissionType.ALLOW)))
                .toList();
    }

    /**
     * Convert Kafka Stream to ACL Bindings.
     *
     * @param kafkaStream The Kafka Stream
     * @return A stream of Kafka ACLs
     * @see <a
     *     href="https://docs.confluent.io/platform/current/streams/developer-guide/security.html#required-acl-setting-for-secure-ak-clusters">Required
     *     ACL setting for secure Kafka clusters</a>
     */
    List<AclBinding> convertKafkaStreamToAclBindings(KafkaStream kafkaStream) {
        Namespace namespace = namespaceService
                .findByName(kafkaStream.getMetadata().getNamespace())
                .orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();

        return List.of(
                // Kafka Stream needs to create & delete changelog/repartition topics with the application id as prefix
                new AclBinding(
                        new ResourcePattern(
                                ResourceType.TOPIC, kafkaStream.getMetadata().getName(), PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                principal, "*", AclOperation.CREATE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(
                                ResourceType.TOPIC, kafkaStream.getMetadata().getName(), PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                principal, "*", AclOperation.DELETE, AclPermissionType.ALLOW)));
    }

    /**
     * Create Kafka ACLs from ACLs.
     *
     * @param toCreate The list of ACLs to create
     */
    void createAcls(List<AccessControlEntry> toCreate) {
        toCreate.forEach(aclToCreate -> managedClusterProperties
                .getAdminClient()
                .createAcls(convertAclToAclBindings(aclToCreate))
                .values()
                .forEach((key, value) -> {
                    try {
                        value.get(managedClusterProperties.getTimeout().getAcl().getCreate(), TimeUnit.MILLISECONDS);

                        Optional<AccessControlEntry> existingAcl = aclService.findByName(
                                aclToCreate.getMetadata().getNamespace(),
                                aclToCreate.getMetadata().getName());
                        AccessControlEntry lastVersion = existingAcl.orElse(aclToCreate);
                        lastVersion
                                .getMetadata()
                                .setGeneration(lastVersion.getMetadata().getGeneration() + 1);

                        boolean isUnchangedSinceLastApply = existingAcl.isEmpty()
                                || !existingAcl
                                        .get()
                                        .getMetadata()
                                        .getUpdateTimestamp()
                                        .after(aclToCreate.getMetadata().getUpdateTimestamp());

                        if (isUnchangedSinceLastApply) {
                            lastVersion.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                        }

                        aclRepository.create(lastVersion);

                        log.atInfo()
                                .addArgument(key)
                                .addArgument(managedClusterProperties.getName())
                                .log("Success creating ACL {} on cluster {}.");
                    } catch (InterruptedException e) {
                        log.error(ERROR, e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        if (isUnchangedSinceLastApply(aclToCreate)) {
                            log.error("Error while creating ACL {} on {}", key, managedClusterProperties.getName(), e);

                            aclToCreate.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                            aclRepository.create(aclToCreate);
                        }
                    }
                }));
    }

    /**
     * Create Kafka ACLs from Kafka Streams.
     *
     * @param toCreate The list of Kafka Streams to create
     */
    void createAclsFromKafkaStreams(List<KafkaStream> toCreate) {
        toCreate.forEach(ksToCreate -> managedClusterProperties
                .getAdminClient()
                .createAcls(convertKafkaStreamToAclBindings(ksToCreate))
                .values()
                .forEach((key, value) -> {
                    try {
                        value.get(managedClusterProperties.getTimeout().getAcl().getCreate(), TimeUnit.MILLISECONDS);

                        Namespace ns = namespaceService
                                .findByName(ksToCreate.getMetadata().getNamespace())
                                .orElseThrow();
                        Optional<KafkaStream> existingKafkaStream = streamService.findByName(
                                ns, ksToCreate.getMetadata().getName());
                        KafkaStream lastVersion = existingKafkaStream.orElse(ksToCreate);
                        lastVersion
                                .getMetadata()
                                .setGeneration(lastVersion.getMetadata().getGeneration() + 1);

                        boolean isUnchangedSinceLastApply = existingKafkaStream.isEmpty()
                                || !existingKafkaStream
                                        .get()
                                        .getMetadata()
                                        .getUpdateTimestamp()
                                        .after(ksToCreate.getMetadata().getUpdateTimestamp());

                        if (isUnchangedSinceLastApply) {
                            lastVersion.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
                        }

                        kafkaStreamRepository.create(lastVersion);

                        log.atInfo()
                                .addArgument(key)
                                .addArgument(managedClusterProperties.getName())
                                .log("Success creating Kafka Stream {} on cluster {}.");
                    } catch (InterruptedException e) {
                        log.error(ERROR, e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        if (isUnchangedSinceLastApply(ksToCreate)) {
                            ksToCreate.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                            kafkaStreamRepository.create(ksToCreate);
                            log.error(
                                    "Error while creating Kafka Stream {} on {}",
                                    key,
                                    managedClusterProperties.getName(),
                                    e);
                        }
                    }
                }));
    }

    /**
     * Delete Kafka ACLs.
     *
     * @param toDelete The list of ACLs to delete
     */
    void deleteAcls(List<AccessControlEntry> toDelete) {
        toDelete.forEach(aclToDelete -> managedClusterProperties
                .getAdminClient()
                .deleteAcls(convertAclToAclBindings(aclToDelete).stream()
                        .map(AclBinding::toFilter)
                        .toList())
                .values()
                .forEach((key, value) -> {
                    try {
                        value.get(managedClusterProperties.getTimeout().getAcl().getDelete(), TimeUnit.MILLISECONDS);
                        if (isUnchangedSinceLastApply(aclToDelete)) {
                            log.info("Success deleting ACL {} on cluster {}.", key, managedClusterProperties.getName());
                            aclRepository.delete(aclToDelete);
                        }
                    } catch (InterruptedException e) {
                        log.error(ERROR, e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        if (isUnchangedSinceLastApply(aclToDelete)) {
                            log.error(
                                    "Error while deleting ACL {} on cluster {}.",
                                    key,
                                    managedClusterProperties.getName(),
                                    e);
                            aclToDelete.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                            aclRepository.create(aclToDelete);
                        }
                    }
                }));
    }

    /**
     * Delete a given list of Kafka Streams.
     *
     * @param toDelete The list of Kafka Streams to delete
     */
    void deleteAclsFromKafkaStreams(List<KafkaStream> toDelete) {
        toDelete.forEach(ksToDelete -> managedClusterProperties
                .getAdminClient()
                .deleteAcls(convertKafkaStreamToAclBindings(ksToDelete).stream()
                        .map(AclBinding::toFilter)
                        .toList())
                .values()
                .forEach((_, value) -> {
                    try {
                        value.get(managedClusterProperties.getTimeout().getAcl().getDelete(), TimeUnit.MILLISECONDS);
                        if (isUnchangedSinceLastApply(ksToDelete)) {
                            log.info(
                                    "Success deleting Kafka Stream {} on cluster {}.",
                                    ksToDelete.getMetadata().getName(),
                                    managedClusterProperties.getName());
                            kafkaStreamRepository.delete(ksToDelete);
                        }
                    } catch (InterruptedException e) {
                        log.error(ERROR, e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        if (isUnchangedSinceLastApply(ksToDelete)) {
                            log.error(
                                    "Error while deleting Kafka Stream {} on cluster {}.",
                                    ksToDelete.getMetadata().getName(),
                                    managedClusterProperties.getName(),
                                    e);
                            ksToDelete.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
                            kafkaStreamRepository.create(ksToDelete);
                        }
                    }
                }));
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
     * Stream that has already been changed.
     *
     * @param kafkaStream The Kafka Stream to deploy
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
