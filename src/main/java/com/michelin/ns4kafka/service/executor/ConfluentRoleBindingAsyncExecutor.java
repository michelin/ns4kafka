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
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.StreamService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingRequest;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingResponse;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Access control entry executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class ConfluentRoleBindingAsyncExecutor {
    private static final String USER_PRINCIPAL = "User:";
    private static final Set<String> MANAGED_ROLES =
            Set.of(DEVELOPER_READ.toString(), DEVELOPER_WRITE.toString(), DEVELOPER_MANAGE.toString());

    private final ManagedClusterProperties managedClusterProperties;
    private final ConfluentCloudClient confluentCloudClient;
    private final AclService aclService;
    private final StreamService streamService;
    private final NamespaceRepository namespaceRepository;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param confluentCloudClient The Confluent Cloud client
     * @param aclService The ACL service
     * @param streamService The stream service
     * @param namespaceRepository The namespace repository
     */
    public ConfluentRoleBindingAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            ConfluentCloudClient confluentCloudClient,
            AclService aclService,
            StreamService streamService,
            NamespaceRepository namespaceRepository) {
        this.managedClusterProperties = managedClusterProperties;
        this.confluentCloudClient = confluentCloudClient;
        this.aclService = aclService;
        this.streamService = streamService;
        this.namespaceRepository = namespaceRepository;
    }

    /**
     * Run the role binding synchronization.
     *
     * @return A mono completing when the synchronization is done
     */
    public Mono<Void> run() {
        if (!this.managedClusterProperties.isManageAcls()
                && this.managedClusterProperties.isConfluentCloud()
                && this.managedClusterProperties.isManageRbac()) {
            return synchronizeRoleBindings();
        }

        return Mono.empty();
    }

    /**
     * Start the role binding synchronization.
     *
     * @return A mono completing when the synchronization is done
     */
    public Mono<Void> synchronizeRoleBindings() {
        log.debug("Starting role binding collection for cluster {}.", managedClusterProperties.getName());

        return collectBrokerRoleBindings()
                .flatMap(brokerRoleBindings -> {
                    Map<Resource, List<RoleBinding>> ns4KafkaRoleBindings = collectNs4KafkaRoleBindings();
                    Set<RoleBindingRequest> ns4KafkaRoleBindingRequests = ns4KafkaRoleBindings.values().stream()
                            .flatMap(List::stream)
                            .map(roleBinding ->
                                    new RoleBindingRequest(roleBinding, managedClusterProperties.getConfluentCloud()))
                            .collect(Collectors.toSet());

                    // Create role bindings before delete to avoid breaking access
                    List<RoleBinding> toCreate = ns4KafkaRoleBindings.values().stream()
                            .flatMap(List::stream)
                            .distinct()
                            .filter(roleBinding -> !brokerRoleBindings.containsKey(
                                    new RoleBindingRequest(roleBinding, managedClusterProperties.getConfluentCloud())))
                            .toList();

                    if (!toCreate.isEmpty()) {
                        log.atDebug()
                                .addArgument(() -> toCreate.stream()
                                        .map(RoleBinding::toString)
                                        .collect(Collectors.joining(",")))
                                .log("Role binding(s) to create: {}.");
                    }

                    return createRoleBindings(toCreate)
                            // Ns4Kafka storage is written in a blocking way, keep it out of the HTTP client event loop
                            .publishOn(Schedulers.boundedElastic())
                            .doOnNext(creationErrors -> updateStatuses(ns4KafkaRoleBindings, toCreate, creationErrors))
                            .then(Mono.defer(() -> {
                                if (!managedClusterProperties.isDropUnsyncAcls()) {
                                    return Mono.empty();
                                }

                                List<RoleBindingResponse> toDelete = brokerRoleBindings.entrySet().stream()
                                        .filter(entry -> !ns4KafkaRoleBindingRequests.contains(entry.getKey()))
                                        .map(Map.Entry::getValue)
                                        .toList();

                                if (!toDelete.isEmpty()) {
                                    log.atDebug()
                                            .addArgument(() -> toDelete.stream()
                                                    .map(RoleBindingResponse::crnPattern)
                                                    .collect(Collectors.joining(",")))
                                            .log("Role binding(s) to delete: {}.");
                                }

                                return deleteRoleBindings(toDelete);
                            }));
                })
                .doOnError(e -> log.error(
                        "An error occurred during the role binding synchronization on cluster {}.",
                        managedClusterProperties.getName(),
                        e))
                .onErrorComplete();
    }

    /**
     * Collect the role bindings of the users managed in Ns4Kafka from Confluent Cloud.
     *
     * @return The role bindings by request
     */
    Mono<Map<RoleBindingRequest, RoleBindingResponse>> collectBrokerRoleBindings() {
        return Mono.defer(() -> {
            // Collect the list of users managed in Ns4Kafka
            Set<String> managedUsers =
                    namespaceRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                            .map(namespace ->
                                    USER_PRINCIPAL + namespace.getSpec().getKafkaUser())
                            .collect(Collectors.toSet());

            return confluentCloudClient
                    .listRoleBindings(
                            managedClusterProperties.getName(),
                            RoleBindingRequest.clusterCrnPattern(managedClusterProperties.getConfluentCloud()) + "*")
                    .filter(response ->
                            MANAGED_ROLES.contains(response.roleName()) && managedUsers.contains(response.principal()))
                    .collectMap(response ->
                            new RoleBindingRequest(response.principal(), response.roleName(), response.crnPattern()));
        });
    }

    /**
     * Collect the role bindings of the Ns4Kafka ACLs and Kafka Streams of the cluster.
     *
     * @return The role bindings by ACL or Kafka Stream
     */
    Map<Resource, List<RoleBinding>> collectNs4KafkaRoleBindings() {
        Map<Resource, List<RoleBinding>> ns4KafkaRoleBindings = new HashMap<>();

        // Skip ACLs and Kafka Streams of namespaces that no longer exist, they cannot be converted
        Set<String> namespaces = namespaceRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                .map(namespace -> namespace.getMetadata().getName())
                .collect(Collectors.toSet());

        // Public ACLs are handled by the ACL executor as Confluent role bindings cannot manage "*"
        ns4KafkaRoleBindings.putAll(aclService.findAllNonPublicForCluster(managedClusterProperties.getName()).stream()
                .filter(acl -> namespaces.contains(acl.getSpec().getGrantedTo()))
                .collect(Collectors.toMap(Function.identity(), this::convertAclToRoleBinding)));

        ns4KafkaRoleBindings.putAll(streamService.findAllForCluster(managedClusterProperties.getName()).stream()
                .filter(kafkaStream ->
                        namespaces.contains(kafkaStream.getMetadata().getNamespace()))
                .collect(Collectors.toMap(
                        Function.identity(), kafkaStream -> List.of(convertKafkaStreamsToRoleBinding(kafkaStream)))));

        return ns4KafkaRoleBindings;
    }

    /**
     * Create role bindings.
     *
     * @param toCreate The list of role bindings to create
     * @return The error message of each role binding that could not be created
     */
    Mono<Map<RoleBinding, String>> createRoleBindings(List<RoleBinding> toCreate) {
        return Mono.defer(() -> {
            Map<RoleBinding, String> creationErrors = new ConcurrentHashMap<>();

            return Flux.fromIterable(toCreate)
                    .concatMap(roleBinding -> confluentCloudClient
                            .createRoleBinding(managedClusterProperties.getName(), roleBinding)
                            .doOnNext(_ -> log.info(
                                    "Success creating role binding {} on cluster {}.",
                                    roleBinding,
                                    managedClusterProperties.getName()))
                            .onErrorResume(e -> {
                                log.error(
                                        "Error while creating role binding {} on cluster {}.",
                                        roleBinding,
                                        managedClusterProperties.getName(),
                                        e);

                                creationErrors.put(
                                        roleBinding,
                                        Objects.toString(
                                                e.getMessage(), e.getClass().getName()));
                                return Mono.empty();
                            }))
                    .then(Mono.fromSupplier(() -> creationErrors));
        });
    }

    /**
     * Update the status of the ACLs and Kafka Streams according to their role bindings.
     *
     * @param ns4KafkaRoleBindings The role bindings by resource
     * @param toCreate The role bindings that had to be created
     * @param creationErrors The error message of each role binding that could not be created
     */
    private void updateStatuses(
            Map<Resource, List<RoleBinding>> ns4KafkaRoleBindings,
            List<RoleBinding> toCreate,
            Map<RoleBinding, String> creationErrors) {
        ns4KafkaRoleBindings.forEach((resource, roleBindings) -> {
            Optional<String> creationError = roleBindings.stream()
                    .filter(creationErrors::containsKey)
                    .map(creationErrors::get)
                    .findFirst();

            // Role bindings already exist and the status is up to date
            if (creationError.isEmpty()
                    && roleBindings.stream().noneMatch(toCreate::contains)
                    && resource.isSuccess()) {
                return;
            }

            if (creationError.isEmpty()) {
                resource.getMetadata().setGeneration(resource.getMetadata().getGeneration() + 1);
            }

            resource.getMetadata()
                    .setStatus(creationError
                            .map(Resource.Metadata.Status::ofFailed)
                            .orElseGet(Resource.Metadata.Status::ofSuccess));

            // Do not overwrite a resource deleted or reapplied since it was read
            if (resource instanceof AccessControlEntry acl && isUnchangedSinceLastApply(acl)) {
                aclService.create(acl);
                return;
            }

            if (resource instanceof KafkaStream ks && isUnchangedSinceLastApply(ks)) {
                streamService.create(ks);
            }
        });
    }

    /**
     * Delete role bindings.
     *
     * @param toDelete The list of role bindings to delete
     * @return A mono completing when the role bindings are deleted
     */
    Mono<Void> deleteRoleBindings(List<RoleBindingResponse> toDelete) {
        // Not possible to batch delete Confluent role bindings
        return Flux.fromIterable(toDelete)
                .concatMap(roleBinding -> confluentCloudClient
                        .deleteRoleBinding(managedClusterProperties.getName(), roleBinding.id())
                        .doOnSuccess(_ -> log.info(
                                "Success deleting role binding {} on cluster {}.",
                                roleBinding,
                                managedClusterProperties.getName()))
                        .onErrorResume(e -> {
                            log.error(
                                    "Error while deleting role binding {} on cluster {}.",
                                    roleBinding,
                                    managedClusterProperties.getName(),
                                    e);
                            return Mono.empty();
                        }))
                .then();
    }

    /**
     * Find the Confluent Cloud role binding matching a role binding and delete it.
     *
     * @param roleBinding The role binding to delete
     * @return The deleted role binding, or empty if none matched
     */
    private Mono<RoleBindingResponse> deleteRoleBinding(RoleBinding roleBinding) {
        RoleBindingRequest request = new RoleBindingRequest(roleBinding, managedClusterProperties.getConfluentCloud());

        return confluentCloudClient
                .listRoleBindings(managedClusterProperties.getName(), request.crnPattern())
                .filter(response -> response.crnPattern().equals(request.crnPattern())
                        && response.principal().equals(request.principal())
                        && response.roleName().equals(request.roleName()))
                .next()
                .flatMap(response ->
                        confluentCloudClient.deleteRoleBinding(managedClusterProperties.getName(), response.id()));
    }

    /**
     * Delete role bindings associated to Ns4Kafka ACLs.
     *
     * @param acls The Ns4Kafka ACLs
     */
    public void deleteRoleBindingsFromAcls(List<AccessControlEntry> acls) {
        acls.forEach(acl -> convertAclToRoleBinding(acl).forEach(roleBinding -> {
            try {
                RoleBindingResponse roleBindingResponse =
                        deleteRoleBinding(roleBinding).block();

                if (roleBindingResponse == null) {
                    log.info(
                            "No role binding to delete for ACL {} on cluster {}.",
                            acl.getMetadata().getName(),
                            managedClusterProperties.getName());
                    return;
                }

                log.info(
                        "Success deleting role binding {} of ACL {} on cluster {}.",
                        roleBindingResponse,
                        acl.getMetadata().getName(),
                        managedClusterProperties.getName());
            } catch (Exception e) {
                log.error(
                        "Error while deleting role binding of ACL {} on cluster {}.",
                        acl.getMetadata().getName(),
                        managedClusterProperties.getName(),
                        e);
            }
        }));
    }

    /**
     * Delete role bindings associated to Ns4Kafka Kafka Streams.
     *
     * @param kafkaStreams The Kafka Streams
     */
    public void deleteRoleBindingsFromKafkaStreams(List<KafkaStream> kafkaStreams) {
        kafkaStreams.forEach(ks -> {
            try {
                RoleBindingResponse roleBindingResponse =
                        deleteRoleBinding(convertKafkaStreamsToRoleBinding(ks)).block();

                if (roleBindingResponse == null) {
                    log.info(
                            "No role binding to delete for Kafka Stream {} on cluster {}.",
                            ks.getMetadata().getName(),
                            managedClusterProperties.getName());
                    return;
                }

                log.info(
                        "Success deleting role binding {} of Kafka Stream {} on cluster {}.",
                        roleBindingResponse,
                        ks.getMetadata().getName(),
                        managedClusterProperties.getName());
            } catch (Exception e) {
                log.error(
                        "Error while deleting role binding of Kafka Stream {} on cluster {}.",
                        ks.getMetadata().getName(),
                        managedClusterProperties.getName(),
                        e);
            }
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
     * Convert Ns4Kafka topic ACL into role binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of role bindings
     */
    List<RoleBinding> convertTopicAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
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
     * Convert Ns4Kafka group ACL into role binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of role bindings
     */
    List<RoleBinding> convertGroupAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();

        if (acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER
                || acl.getSpec().getPermission() == AccessControlEntry.Permission.READ) {
            return List.of(new RoleBinding(principal, DEVELOPER_READ, GROUP, computeResourcePattern(acl)));
        }

        return List.of();
    }

    /**
     * Convert Ns4Kafka connect ACL into role binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of role bindings
     */
    List<RoleBinding> convertConnectAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = "connect-" + computeResourcePattern(acl);

        if (acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
            return List.of(new RoleBinding(principal, DEVELOPER_READ, GROUP, resource));
        }

        return List.of();
    }

    /**
     * Convert Ns4Kafka transactional ID ACL into role binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of role bindings
     */
    List<RoleBinding> convertTransAclToRoleBinding(AccessControlEntry acl) {
        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
        String resource = computeResourcePattern(acl);

        if (acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER
                || acl.getSpec().getPermission() == AccessControlEntry.Permission.WRITE) {
            return List.of(new RoleBinding(principal, DEVELOPER_WRITE, TRANSACTIONAL_ID, resource));
        }

        return List.of();
    }

    /**
     * Convert Ns4Kafka ACL into role binding.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of role bindings
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
     * Convert Kafka Stream into role binding.
     *
     * @param stream The Kafka Stream resource
     * @return A role binding
     */
    RoleBinding convertKafkaStreamsToRoleBinding(KafkaStream stream) {
        Namespace namespace = namespaceRepository
                .findByName(stream.getMetadata().getNamespace())
                .orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();

        return new RoleBinding(
                principal, DEVELOPER_MANAGE, TOPIC, stream.getMetadata().getName() + "*");
    }

    /**
     * Check the ACL has been neither deleted nor reapplied since it was read.
     *
     * @param acl The synchronized ACL
     * @return True if unchanged, false otherwise
     */
    private boolean isUnchangedSinceLastApply(AccessControlEntry acl) {
        Optional<AccessControlEntry> existingAcl = aclService.findByName(
                acl.getMetadata().getNamespace(), acl.getMetadata().getName());

        return existingAcl.isPresent()
                && (existingAcl.get().getMetadata().getUpdateTimestamp() == null
                        || (acl.getMetadata().getUpdateTimestamp() != null
                                && !existingAcl
                                        .get()
                                        .getMetadata()
                                        .getUpdateTimestamp()
                                        .after(acl.getMetadata().getUpdateTimestamp())));
    }

    /**
     * Check the Kafka Stream has been neither deleted nor reapplied since it was read.
     *
     * @param kafkaStream The synchronized Kafka Stream
     * @return True if unchanged, false otherwise
     */
    private boolean isUnchangedSinceLastApply(KafkaStream kafkaStream) {
        Optional<KafkaStream> existingStream = namespaceRepository
                .findByName(kafkaStream.getMetadata().getNamespace())
                .flatMap(namespace -> streamService.findByName(
                        namespace, kafkaStream.getMetadata().getName()));

        return existingStream.isPresent()
                && (existingStream.get().getMetadata().getUpdateTimestamp() == null
                        || (kafkaStream.getMetadata().getUpdateTimestamp() != null
                                && !existingStream
                                        .get()
                                        .getMetadata()
                                        .getUpdateTimestamp()
                                        .after(kafkaStream.getMetadata().getUpdateTimestamp())));
    }
}
