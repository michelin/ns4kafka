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
import static com.michelin.ns4kafka.service.AclService.PUBLIC_GRANTED_TO;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.StreamService;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

/**
 * Access control entry executor.
 */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
@AllArgsConstructor
public class AccessControlEntryAsyncExecutor {
    private static final String USER_PRINCIPAL = "User:";

    private final ManagedClusterProperties managedClusterProperties;

    private AclService aclService;

    private StreamService streamService;

    private NamespaceRepository namespaceRepository;

    /**
     * Run the ACLs synchronization.
     */
    public void run() {
        if (this.managedClusterProperties.isManageAcls()) {
            synchronizeAcls();
        }
    }

    /**
     * Start the ACLs synchronization.
     */
    private void synchronizeAcls() {
        log.debug("Starting ACL collection for cluster {}", managedClusterProperties.getName());

        try {
            // List ACLs from broker
            List<AclBinding> brokerAcls = collectBrokerAcls(true);

            // List ACLs from NS4Kafka
            List<AclBinding> ns4kafkaAcls = collectNs4KafkaAcls();

            List<AclBinding> toCreate = ns4kafkaAcls.stream()
                .filter(aclBinding -> !brokerAcls.contains(aclBinding))
                .toList();

            List<AclBinding> toDelete = brokerAcls.stream()
                .filter(aclBinding -> !ns4kafkaAcls.contains(aclBinding))
                .toList();

            if (!toCreate.isEmpty()) {
                log.debug(
                    "ACL(s) to create: " + String.join(",", toCreate.stream().map(AclBinding::toString).toList()));
            }

            if (!toDelete.isEmpty()) {
                if (!managedClusterProperties.isDropUnsyncAcls()) {
                    log.debug("The ACL drop is disabled. The following ACLs won't be deleted.");
                }
                log.debug(
                    "ACL(s) to delete: " + String.join(",", toDelete.stream().map(AclBinding::toString).toList()));
            }

            // Execute toAdd list BEFORE toDelete list to avoid breaking ACL on connected user
            // such as deleting <LITERAL "toto.titi"> only to add one second later <PREFIX "toto.">
            createAcls(toCreate);

            if (managedClusterProperties.isDropUnsyncAcls()) {
                deleteAcls(toDelete);
            }
        } catch (KafkaStoreException | ExecutionException | TimeoutException e) {
            log.error("An error occurred collecting ACLs from broker during ACLs synchronization", e);
        } catch (InterruptedException e) {
            log.error("An error occurred during ACLs synchronization", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Collect the ACLs from Ns4Kafka.
     * Whenever the permission is OWNER, create 2 entries (one READ and one WRITE)
     * This is necessary to translate Ns4Kafka grouped AccessControlEntry (OWNER, WRITE, READ)
     * into Kafka Atomic ACLs (READ and WRITE)
     *
     * @return A list of ACLs
     */
    private List<AclBinding> collectNs4KafkaAcls() {
        List<Namespace> namespaces = namespaceRepository.findAllForCluster(managedClusterProperties.getName());

        // Converts topic, group and transaction Ns4Kafka ACLs to topic and group Kafka AclBindings
        Stream<AclBinding> aclBindingsFromAcls = namespaces
            .stream()
            .flatMap(namespace -> aclService.findAllGrantedToNamespace(namespace)
                .stream()
                .filter(accessControlEntry -> List.of(TOPIC, GROUP, TRANSACTIONAL_ID)
                    .contains(accessControlEntry.getSpec().getResourceType()))
                .flatMap(accessControlEntry -> convertAccessControlEntryToAclBinding(accessControlEntry)
                    .stream())
                .distinct());

        // Converts KafkaStream resources to topic (CREATE/DELETE) AclBindings
        Stream<AclBinding> aclBindingFromKstream = namespaces.stream()
            .flatMap(namespace -> streamService.findAllForNamespace(namespace)
                .stream()
                .flatMap(kafkaStream ->
                    buildAclBindingsFromKafkaStream(kafkaStream, namespace.getSpec().getKafkaUser()).stream()));

        // Converts connect ACLs to group AclBindings (connect-)
        Stream<AclBinding> aclBindingFromConnect = namespaces
            .stream()
            .flatMap(namespace -> aclService.findAllGrantedToNamespace(namespace)
                .stream()
                .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType()
                    == AccessControlEntry.ResourceType.CONNECT)
                .filter(accessControlEntry -> accessControlEntry.getSpec().getPermission()
                    == AccessControlEntry.Permission.OWNER)
                .map(this::convertConnectorAccessControlEntryToAclBinding));

        List<AclBinding> ns4kafkaAcls = Stream.of(aclBindingsFromAcls, aclBindingFromKstream, aclBindingFromConnect)
            .flatMap(Function.identity())
            .toList();

        if (!ns4kafkaAcls.isEmpty()) {
            log.trace("ACL(s) found in Ns4Kafka: "
                + String.join(",", ns4kafkaAcls.stream().map(AclBinding::toString).toList()));
        }

        return ns4kafkaAcls;
    }

    /**
     * Collect the ACLs from broker.
     *
     * @param managedUsersOnly Only retrieve ACLs from Kafka user managed by Ns4Kafka or not?
     * @return A list of ACLs
     * @throws ExecutionException   Any execution exception during ACLs description
     * @throws InterruptedException Any interrupted exception during ACLs description
     * @throws TimeoutException     Any timeout exception during ACLs description
     */
    private List<AclBinding> collectBrokerAcls(boolean managedUsersOnly)
        throws ExecutionException, InterruptedException, TimeoutException {
        List<ResourceType> validResourceTypes = List.of(
            org.apache.kafka.common.resource.ResourceType.TOPIC,
            org.apache.kafka.common.resource.ResourceType.GROUP,
            org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID
        );

        AccessControlEntryFilter accessControlEntryFilter = new AccessControlEntryFilter(
            managedClusterProperties.getProvider()
                .equals(ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD) ? "UserV2:*" : null,
            null,
            AclOperation.ANY,
            AclPermissionType.ANY
        );
        AclBindingFilter aclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY, accessControlEntryFilter);

        List<AclBinding> userAcls = getAdminClient()
            .describeAcls(aclBindingFilter)
            .values()
            .get(managedClusterProperties.getTimeout().getAcl().getDescribe(), TimeUnit.MILLISECONDS)
            .stream()
            .filter(aclBinding -> validResourceTypes.contains(aclBinding.pattern().resourceType()))
            .toList();

        if (managedUsersOnly) {
            // Collect the list of users managed in Ns4Kafka
            List<String> managedUsers = new ArrayList<>();
            managedUsers.add(USER_PRINCIPAL + PUBLIC_GRANTED_TO);
            managedUsers.addAll(namespaceRepository.findAllForCluster(managedClusterProperties.getName())
                .stream()
                .flatMap(namespace -> Stream.of(USER_PRINCIPAL + namespace.getSpec().getKafkaUser()))
                .toList());

            // Filter out the ACLs to retain only those matching
            userAcls = userAcls
                .stream()
                .filter(aclBinding -> managedUsers.contains(aclBinding.entry().principal()))
                .toList();

            if (!userAcls.isEmpty()) {
                log.trace("ACL(s) found in broker (managed scope): "
                    + String.join(",", userAcls.stream().map(AclBinding::toString).toList()));
            }
        }

        if (!userAcls.isEmpty()) {
            log.trace(
                "ACL(s) found in broker: " + String.join(",", userAcls.stream().map(AclBinding::toString).toList()));
        }

        return userAcls;
    }

    /**
     * Convert Ns4Kafka topic, group and transactional ACL into Kafka ACL.
     *
     * @param accessControlEntry The Ns4Kafka ACL
     * @return A list of Kafka ACLs
     */
    private List<AclBinding> convertAccessControlEntryToAclBinding(AccessControlEntry accessControlEntry) {
        // Convert pattern, convert resource type from Ns4Kafka to org.apache.kafka.common types
        PatternType patternType = PatternType.fromString(
            accessControlEntry.getSpec().getResourcePatternType().toString());

        ResourceType resourceType = ResourceType.fromString(
            accessControlEntry.getSpec().getResourceType().toString());

        ResourcePattern resourcePattern = new ResourcePattern(resourceType,
            accessControlEntry.getSpec().getResource(), patternType);

        // Generate the required AclOperation and principal based on the permission
        List<AclOperation> aclOperations;
        if (accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
            aclOperations = computeAclOperationForOwner(resourceType);
        } else {
            aclOperations = List.of(
                AclOperation.fromString(accessControlEntry.getSpec().getPermission().toString())
            );
        }

        String kafkaUser;
        if (accessControlEntry.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO)) {
            kafkaUser = PUBLIC_GRANTED_TO;
        } else {
            kafkaUser = namespaceRepository.findByName(accessControlEntry.getSpec().getGrantedTo())
                .orElseThrow()
                .getSpec()
                .getKafkaUser();
        }

        return aclOperations
            .stream()
            .map(aclOperation -> new AclBinding(
                resourcePattern,
                new org.apache.kafka.common.acl.AccessControlEntry(
                    USER_PRINCIPAL + kafkaUser,
                    "*",
                    aclOperation,
                    AclPermissionType.ALLOW
                )
            ))
            .toList();
    }

    /**
     * Convert Kafka Stream resource into Kafka ACL.
     *
     * @param stream    The Kafka Stream resource
     * @param kafkaUser The ACL owner
     * @return A list of Kafka ACLs
     */
    private List<AclBinding> buildAclBindingsFromKafkaStream(KafkaStream stream, String kafkaUser) {
        // As per https://docs.confluent.io/platform/current/streams/developer-guide/security.html#required-acl-setting-for-secure-ak-clusters
        return List.of(
            // CREATE and DELETE on Stream Topics
            new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, stream.getMetadata().getName(),
                    PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(USER_PRINCIPAL + kafkaUser, "*", AclOperation.CREATE,
                    AclPermissionType.ALLOW)
            ),
            new AclBinding(
                new ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC, stream.getMetadata().getName(),
                    PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(USER_PRINCIPAL + kafkaUser, "*", AclOperation.DELETE,
                    AclPermissionType.ALLOW)
            ),
            // WRITE on TransactionalId
            new AclBinding(
                new ResourcePattern(
                    org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID, stream.getMetadata().getName(),
                    PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(USER_PRINCIPAL + kafkaUser, "*", AclOperation.WRITE,
                    AclPermissionType.ALLOW)
            )
        );
    }

    /**
     * Convert Ns4Kafka connect ACL into Kafka ACL.
     *
     * @param accessControlEntry The Ns4Kafka ACL
     * @return A list of Kafka ACLs
     */
    private AclBinding convertConnectorAccessControlEntryToAclBinding(AccessControlEntry accessControlEntry) {
        PatternType patternType = PatternType.fromString(
            accessControlEntry.getSpec().getResourcePatternType().toString()
        );

        ResourcePattern resourcePattern = new ResourcePattern(
            org.apache.kafka.common.resource.ResourceType.GROUP,
            "connect-" + accessControlEntry.getSpec().getResource(),
            patternType
        );

        String kafkaUser = namespaceRepository.findByName(accessControlEntry.getSpec().getGrantedTo())
            .orElseThrow()
            .getSpec()
            .getKafkaUser();

        return new AclBinding(
            resourcePattern,
            new org.apache.kafka.common.acl.AccessControlEntry(
                USER_PRINCIPAL + kafkaUser,
                "*",
                AclOperation.READ,
                AclPermissionType.ALLOW)
        );
    }

    /**
     * Get ACL operations from given resource type.
     *
     * @param resourceType The resource type
     * @return A list of ACL operations
     */
    private List<AclOperation> computeAclOperationForOwner(ResourceType resourceType) {
        return switch (resourceType) {
            case TOPIC -> List.of(AclOperation.WRITE, AclOperation.READ, AclOperation.DESCRIBE_CONFIGS);
            case GROUP -> List.of(AclOperation.READ);
            case TRANSACTIONAL_ID -> List.of(AclOperation.DESCRIBE, AclOperation.WRITE);
            default -> throw new IllegalArgumentException("Not implemented yet: " + resourceType);
        };
    }

    /**
     * Delete a given list of ACLs.
     *
     * @param toDelete The list of ACLs to delete
     */
    private void deleteAcls(List<AclBinding> toDelete) {
        getAdminClient()
            .deleteAcls(toDelete
                .stream()
                .map(AclBinding::toFilter)
                .toList())
            .values()
            .forEach((key, value) -> {
                try {
                    value.get(managedClusterProperties.getTimeout().getAcl().getDelete(), TimeUnit.MILLISECONDS);
                    log.info("Success deleting ACL {} on {}", key, managedClusterProperties.getName());
                } catch (InterruptedException e) {
                    log.error("Error", e);
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error(
                        String.format("Error while deleting ACL %s on %s", key, managedClusterProperties.getName()),
                        e
                    );
                }
            });
    }

    /**
     * Delete a given Ns4Kafka ACL.
     * Convert Ns4Kafka ACL into Kafka ACLs before deletion.
     *
     * @param accessControlEntry The ACL
     */
    public void deleteAcl(AccessControlEntry accessControlEntry) {
        if (managedClusterProperties.isManageAcls()) {
            List<AclBinding> results = new ArrayList<>();

            if (List.of(TOPIC, GROUP, TRANSACTIONAL_ID).contains(accessControlEntry.getSpec().getResourceType())) {
                results.addAll(convertAccessControlEntryToAclBinding(accessControlEntry));
            }

            if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT
                && accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
                results.add(convertConnectorAccessControlEntryToAclBinding(accessControlEntry));
            }

            deleteAcls(results);
        }
    }

    /**
     * Delete a given Kafka Streams.
     *
     * @param namespace   The namespace
     * @param kafkaStream The Kafka Streams
     */
    public void deleteKafkaStreams(Namespace namespace, KafkaStream kafkaStream) {
        if (managedClusterProperties.isManageAcls()) {
            List<AclBinding> results = new ArrayList<>(buildAclBindingsFromKafkaStream(
                kafkaStream,
                namespace.getSpec().getKafkaUser())
            );
            deleteAcls(results);
        }
    }

    /**
     * Create a given list of ACLs.
     *
     * @param toCreate The list of ACLs to create
     */
    private void createAcls(List<AclBinding> toCreate) {
        getAdminClient()
            .createAcls(toCreate)
            .values()
            .forEach((key, value) -> {
                try {
                    value.get(managedClusterProperties.getTimeout().getAcl().getCreate(), TimeUnit.MILLISECONDS);
                    log.info("Success creating ACL {} on {}", key, managedClusterProperties.getName());
                } catch (InterruptedException e) {
                    log.error("Error", e);
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error(
                        String.format("Error while creating ACL %s on %s", key, managedClusterProperties.getName()),
                        e
                    );
                }
            });
    }

    /**
     * Getter for admin client service.
     *
     * @return The admin client
     */
    private Admin getAdminClient() {
        return managedClusterProperties.getAdminClient();
    }
}
