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
import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.GROUP;
import static com.michelin.ns4kafka.model.AccessControlEntry.ResourceType.TOPIC;
import static com.michelin.ns4kafka.service.AclService.PUBLIC_GRANTED_TO;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.AccessControlEntryRepository;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.StreamRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.StreamService;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

/** Access control entry executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class AccessControlEntryAsyncExecutor {
    private static final String USER_PRINCIPAL = "User:";
    private static final String USER_PRINCIPAL_PUBLIC = "User:*";
    private static final String USER_PRINCIPAL_PUBLIC_V2 = "UserV2:*";

    private static final Set<AccessControlEntry.ResourceType> TOPIC_GROUP_RESOURCE_TYPES = EnumSet.of(TOPIC, GROUP);
    private static final Set<ResourceType> VALID_RESOURCE_TYPES =
            EnumSet.of(ResourceType.TOPIC, ResourceType.GROUP, ResourceType.TRANSACTIONAL_ID);
    private static final Set<AclOperation> TOPIC_ACL_OPERATIONS =
            EnumSet.of(AclOperation.WRITE, AclOperation.READ, AclOperation.DESCRIBE_CONFIGS);
    private static final Set<AclOperation> GROUP_ACL_OPERATIONS = EnumSet.of(AclOperation.READ);

    private final ManagedClusterProperties managedClusterProperties;
    private final AclService aclService;
    private final StreamService streamService;
    private final NamespaceRepository namespaceRepository;
    private final AccessControlEntryRepository aclRepository;
    private final StreamRepository streamRepository;
    private final AclBindingFilter aclBindingFilter;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param aclService The ACL service
     * @param streamService The stream service
     * @param namespaceRepository The namespace repository
     * @param aclRepository The ACL repository
     * @param streamRepository The Kafka Stream repository
     */
    public AccessControlEntryAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            AclService aclService,
            StreamService streamService,
            NamespaceRepository namespaceRepository,
            AccessControlEntryRepository aclRepository,
            StreamRepository streamRepository) {
        this.managedClusterProperties = managedClusterProperties;
        this.aclService = aclService;
        this.streamService = streamService;
        this.namespaceRepository = namespaceRepository;
        this.aclRepository = aclRepository;
        this.streamRepository = streamRepository;

        AccessControlEntryFilter accessControlEntryFilter = new AccessControlEntryFilter(
                managedClusterProperties.isConfluentCloud() ? USER_PRINCIPAL_PUBLIC_V2 : null,
                null,
                AclOperation.ANY,
                AclPermissionType.ANY);

        this.aclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY, accessControlEntryFilter);
    }

    /** Run the ACLs synchronization. */
    public void run() {
        if (this.managedClusterProperties.isManageAcls() || this.managedClusterProperties.isManageRbac()) {
            synchronizeAcls();
        }
    }

    /** Start the ACLs synchronization. */
    private void synchronizeAcls() {
        log.debug("Starting ACL collection for cluster {}", managedClusterProperties.getName());

        try {
            Set<AclBinding> brokerAcls = collectBrokerAcls();
            Set<AclBinding> ns4KafkaAcls = collectNs4KafkaAcls();

            // Add ACLs before delete to avoid breaking ACL
            // such as deleting <LITERAL "toto.titi"> only to add one second later <PREFIX "toto.">
            List<AclBinding> toCreate = ns4KafkaAcls.stream()
                    .filter(aclBinding -> !brokerAcls.contains(aclBinding))
                    .toList();

            Set<AclBinding> created = new HashSet<>();
            Map<AclBinding, String> creationErrors = new HashMap<>();

            if (!toCreate.isEmpty()) {
                log.atDebug()
                        .addArgument(() ->
                                toCreate.stream().map(AclBinding::toString).collect(Collectors.joining(",")))
                        .log("ACL(s) to create: {}");

                Map<Boolean, List<AclBinding>> partitions = toCreate.stream()
                        .collect(Collectors.partitioningBy(aclBinding ->
                                PUBLIC_GRANTED_TO.equals(aclBinding.entry().principal())));

                // Create Kafka ACL only for public ACLs because not possible with Confluent role bindings
                List<AclBinding> publicAclsToCreate = partitions.get(true);
                createAcls(publicAclsToCreate, created, creationErrors);

                if (managedClusterProperties.isManageAcls()) {
                    List<AclBinding> nonPublicAclsToCreate = partitions.get(false);
                    createAcls(nonPublicAclsToCreate, created, creationErrors);
                }
            }

            Set<AclBinding> deployed = new HashSet<>(brokerAcls);
            deployed.addAll(created);
            updateStatuses(deployed, created, creationErrors);

            if (managedClusterProperties.isManageAcls() && managedClusterProperties.isDropUnsyncAcls()) {
                List<AclBinding> toDelete = brokerAcls.stream()
                        .filter(aclBinding -> !ns4KafkaAcls.contains(aclBinding))
                        .toList();

                if (!toDelete.isEmpty()) {
                    log.atDebug()
                            .addArgument(() ->
                                    toDelete.stream().map(AclBinding::toString).collect(Collectors.joining(",")))
                            .log("ACL(s) to delete: {}");

                    deleteAcls(toDelete);
                }
            }
        } catch (KafkaStoreException | ExecutionException | TimeoutException e) {
            log.error("An error occurred collecting ACLs from broker during ACLs synchronization", e);
        } catch (InterruptedException e) {
            log.error("An error occurred during ACLs synchronization", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Collect the ACLs from Ns4Kafka. Whenever the permission is OWNER, create 2 entries (one READ and one WRITE) This
     * is necessary to translate Ns4Kafka grouped AccessControlEntry (OWNER, WRITE, READ) into Kafka Atomic ACLs (READ
     * and WRITE)
     *
     * @return A set of ACLs
     */
    private Set<AclBinding> collectNs4KafkaAcls() {
        Stream<AclBinding> aclBindings = aclService.findAllForCluster(managedClusterProperties.getName()).stream()
                .flatMap(acl -> convertToAclBindings(acl).stream());

        // Converts KafkaStream resources to topic (CREATE/DELETE) AclBindings
        // Looping over namespaces because some Kafka Streams might have a non-existing namespace
        Stream<AclBinding> streamAclBindings =
                namespaceRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                        .flatMap(namespace -> {
                            String principal =
                                    USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
                            return streamService.findAllForNamespace(namespace).stream()
                                    .flatMap(kafkaStream -> buildAclBindingsFromKafkaStream(kafkaStream, principal));
                        });

        Set<AclBinding> ns4KafkaAcls =
                Stream.concat(aclBindings, streamAclBindings).collect(Collectors.toSet());

        if (!ns4KafkaAcls.isEmpty()) {
            log.atTrace()
                    .addArgument(() ->
                            ns4KafkaAcls.stream().map(AclBinding::toString).collect(Collectors.joining(",")))
                    .log("ACL(s) found in Ns4Kafka: {}");
        }

        return ns4KafkaAcls;
    }

    /**
     * Convert a Ns4Kafka ACL into the Kafka ACLs deployed by this executor.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Kafka ACLs
     */
    private List<AclBinding> convertToAclBindings(AccessControlEntry acl) {
        // Converts topic and group Ns4Kafka ACLs to topic & group & transactional AclBindings
        if (TOPIC_GROUP_RESOURCE_TYPES.contains(acl.getSpec().getResourceType())) {
            return convertAclToAclBindings(acl);
        }

        // Converts connector ACLs to group AclBindings (connect-)
        if (acl.getSpec().getResourceType() == CONNECT
                && acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
            return List.of(convertConnectorAclToAclBinding(acl));
        }

        return List.of();
    }

    /**
     * Update the status of the Ns4Kafka ACLs and Kafka Streams deployed by this executor.
     *
     * @param deployed The Kafka ACLs existing on the broker
     * @param created The Kafka ACLs created during this synchronization
     * @param creationErrors The error message of each Kafka ACL that could not be created
     */
    private void updateStatuses(
            Set<AclBinding> deployed, Set<AclBinding> created, Map<AclBinding, String> creationErrors) {
        // Non-public ACLs are handled by the Confluent role binding executor when the cluster does not manage ACLs
        aclService.findAllForCluster(managedClusterProperties.getName()).stream()
                .filter(acl -> managedClusterProperties.isManageAcls() || aclService.isPublicAcl(acl))
                .forEach(acl -> updateStatus(acl, () -> convertToAclBindings(acl), deployed, created, creationErrors));

        if (managedClusterProperties.isManageAcls()) {
            namespaceRepository
                    .findAllForCluster(managedClusterProperties.getName())
                    .forEach(namespace -> {
                        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
                        streamService
                                .findAllForNamespace(namespace)
                                .forEach(kafkaStream -> updateStatus(
                                        kafkaStream,
                                        () -> buildAclBindingsFromKafkaStream(kafkaStream, principal)
                                                .toList(),
                                        deployed,
                                        created,
                                        creationErrors));
                    });
        }
    }

    /**
     * Update the status of an ACL or a Kafka Stream according to its Kafka ACLs.
     *
     * @param resource The ACL or Kafka Stream
     * @param converter The conversion into Kafka ACLs
     * @param deployed The Kafka ACLs existing on the broker
     * @param created The Kafka ACLs created during this synchronization
     * @param creationErrors The error message of each Kafka ACL that could not be created
     */
    private void updateStatus(
            Resource resource,
            Supplier<List<AclBinding>> converter,
            Set<AclBinding> deployed,
            Set<AclBinding> created,
            Map<AclBinding, String> creationErrors) {
        try {
            List<AclBinding> aclBindings = converter.get();
            Optional<String> creationError = aclBindings.stream()
                    .filter(creationErrors::containsKey)
                    .map(creationErrors::get)
                    .findFirst();

            if (creationError.isPresent()) {
                resource.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(creationError.get()));
            } else {
                // Not deployed yet
                if (!deployed.containsAll(aclBindings)) {
                    return;
                }

                // Kafka ACLs already exist and the status is up to date
                if (aclBindings.stream().noneMatch(created::contains) && resource.isSuccess()) {
                    return;
                }

                resource.getMetadata().setGeneration(resource.getMetadata().getGeneration() + 1);
                resource.getMetadata().setStatus(Resource.Metadata.Status.ofSuccess());
            }
        } catch (Exception e) {
            log.error(
                    "Error while converting {} {} to ACLs on cluster {}.",
                    resource.getKind(),
                    resource.getMetadata().getName(),
                    managedClusterProperties.getName(),
                    e);

            resource.getMetadata().setStatus(Resource.Metadata.Status.ofFailed(e.getMessage()));
        }

        // Do not overwrite a resource deleted or reapplied since it was read
        if (resource instanceof AccessControlEntry acl
                && isUnchangedSinceLastApply(
                        acl,
                        aclService.findByName(
                                acl.getMetadata().getNamespace(),
                                acl.getMetadata().getName()))) {
            aclRepository.create(acl);
        } else if (resource instanceof KafkaStream ks
                && isUnchangedSinceLastApply(
                        ks,
                        namespaceRepository
                                .findByName(ks.getMetadata().getNamespace())
                                .flatMap(namespace -> streamService.findByName(
                                        namespace, ks.getMetadata().getName())))) {
            streamRepository.create(ks);
        }
    }

    /**
     * Check the resource has been neither deleted nor reapplied since it was read.
     *
     * @param resource The synchronized resource
     * @param existingResource The resource currently stored
     * @return True if unchanged, false otherwise
     */
    private boolean isUnchangedSinceLastApply(Resource resource, Optional<? extends Resource> existingResource) {
        return existingResource.isPresent()
                && (existingResource.get().getMetadata().getUpdateTimestamp() == null
                        || (resource.getMetadata().getUpdateTimestamp() != null
                                && !existingResource
                                        .get()
                                        .getMetadata()
                                        .getUpdateTimestamp()
                                        .after(resource.getMetadata().getUpdateTimestamp())));
    }

    /**
     * Collect the ACLs from broker.
     *
     * @return A set of ACLs
     * @throws ExecutionException Any execution exception during ACLs description
     * @throws InterruptedException Any interrupted exception during ACLs description
     * @throws TimeoutException Any timeout exception during ACLs description
     */
    private Set<AclBinding> collectBrokerAcls() throws ExecutionException, InterruptedException, TimeoutException {
        // Collect the list of users managed in Ns4Kafka
        Set<String> managedUsers = namespaceRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                .map(namespace -> USER_PRINCIPAL + namespace.getSpec().getKafkaUser())
                .collect(Collectors.toSet());
        managedUsers.add(USER_PRINCIPAL_PUBLIC);

        return getAdminClient()
                .describeAcls(aclBindingFilter)
                .values()
                .get(managedClusterProperties.getTimeout().getAcl().getDescribe(), TimeUnit.MILLISECONDS)
                .stream()
                .filter(aclBinding ->
                        VALID_RESOURCE_TYPES.contains(aclBinding.pattern().resourceType())
                                && managedUsers.contains(aclBinding.entry().principal()))
                .collect(Collectors.toSet());
    }

    /**
     * Convert Ns4Kafka topic and group ACL into Kafka ACL.
     *
     * @param acl The Ns4Kafka ACL
     * @return A list of Kafka ACLs
     */
    private List<AclBinding> convertAclToAclBindings(AccessControlEntry acl) {
        // Convert pattern & resource type from Ns4Kafka to org.apache.kafka.common types
        PatternType patternType =
                PatternType.fromString(acl.getSpec().getResourcePatternType().toString());

        ResourceType resourceType =
                ResourceType.fromString(acl.getSpec().getResourceType().toString());

        ResourcePattern resourcePattern =
                new ResourcePattern(resourceType, acl.getSpec().getResource(), patternType);

        // Generate the required AclOperation and principal based on the permission
        Set<AclOperation> aclOperations;
        if (acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
            aclOperations = switch (resourceType) {
                case TOPIC -> TOPIC_ACL_OPERATIONS;
                case GROUP -> GROUP_ACL_OPERATIONS;
                default -> throw new IllegalArgumentException("Not implemented yet: " + resourceType);
            };
        } else {
            aclOperations = EnumSet.of(
                    AclOperation.fromString(acl.getSpec().getPermission().toString()));
        }

        if (aclService.isPublicAcl(acl)) {
            return aclOperations.stream()
                    .map(aclOperation -> new AclBinding(
                            resourcePattern,
                            new org.apache.kafka.common.acl.AccessControlEntry(
                                    USER_PRINCIPAL_PUBLIC, "*", aclOperation, AclPermissionType.ALLOW)))
                    .toList();
        }

        Namespace namespace =
                namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
        String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();

        // At most 5 ACLs will be generated (owner of ACL GROUP generates the most)
        List<AclBinding> results = new ArrayList<>(5);
        aclOperations.forEach(aclOperation -> results.add(new AclBinding(
                resourcePattern,
                new org.apache.kafka.common.acl.AccessControlEntry(
                        principal, "*", aclOperation, AclPermissionType.ALLOW))));

        if (GROUP.equals(acl.getSpec().getResourceType())) {
            if (namespace.getSpec().isTransactionsEnabled()) {
                addEosConnectorAclBindings(results, acl, principal);
            } else if (streamService.hasKafkaStream(namespace)) {
                addKafkaStreamAclBindings(results, acl, principal);
            }
        }

        return results;
    }

    /**
     * Convert Kafka Stream to ACL Bindings.
     *
     * @param stream The Kafka Stream resource
     * @param principal The Kafka principal
     * @return A stream of Kafka ACLs
     * @see <a
     *     href="https://docs.confluent.io/platform/current/streams/developer-guide/security.html#required-acl-setting-for-secure-ak-clusters">Required
     *     ACL setting for secure Kafka clusters</a>
     */
    private Stream<AclBinding> buildAclBindingsFromKafkaStream(KafkaStream stream, String principal) {
        return Stream.of(
                // Kafka Stream needs to create & delete changelog/repartition topics with the application id as prefix
                new AclBinding(
                        new ResourcePattern(
                                ResourceType.TOPIC, stream.getMetadata().getName(), PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                principal, "*", AclOperation.CREATE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(
                                ResourceType.TOPIC, stream.getMetadata().getName(), PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                principal, "*", AclOperation.DELETE, AclPermissionType.ALLOW)));
    }

    /**
     * Build Transactional ID Kafka ACLs from the given Ns4Kafka Group ACL, for Kafka Streams.
     *
     * @param results The list to which the generated ACLs will be added
     * @param acl The Ns4Kafka group ACL
     * @param principal The Kafka User
     * @see <a
     *     href="https://docs.confluent.io/platform/current/streams/developer-guide/security.html#required-acl-setting-for-secure-ak-clusters">Required
     *     ACL setting for secure Kafka clusters</a>
     */
    private void addKafkaStreamAclBindings(List<AclBinding> results, AccessControlEntry acl, String principal) {
        // PREFIXED ACLs to cover all Kafka Streams & EOS connectors.
        results.add(new AclBinding(
                new ResourcePattern(ResourceType.TRANSACTIONAL_ID, acl.getSpec().getResource(), PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW)));

        results.add(new AclBinding(
                new ResourcePattern(ResourceType.TRANSACTIONAL_ID, acl.getSpec().getResource(), PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    /**
     * Build Transactional ID Kafka ACLs from the given Ns4Kafka Group ACL, to allow transactions for EOS connectors.
     *
     * @param results The list to which the generated ACLs will be added
     * @param acl The Ns4Kafka group ACL
     * @param principal The Kafka user
     * @see <a
     *     href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=153816406#KIP618:ExactlyOnceSupportforSourceConnectors-Workerprincipalpermissions">ExactlyOnceSupportforSourceConnectors</a>
     */
    private void addEosConnectorAclBindings(List<AclBinding> results, AccessControlEntry acl, String principal) {
        String connectClusterPrefix = "connect-cluster-" + acl.getSpec().getResource();
        String resource = acl.getSpec().getResource();

        // EOS connectors need "write" and "describe" on "connect-cluster-${groupId}".
        results.add(new AclBinding(
                new ResourcePattern(ResourceType.TRANSACTIONAL_ID, connectClusterPrefix, PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW)));

        results.add(new AclBinding(
                new ResourcePattern(ResourceType.TRANSACTIONAL_ID, connectClusterPrefix, PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)));

        // EOS connectors need "write" and "describe" on "${groupId}-${connector}-${taskId}".
        // PREFIXED ACLs to cover all Kafka Streams & EOS connectors.
        results.add(new AclBinding(
                new ResourcePattern(ResourceType.TRANSACTIONAL_ID, resource, PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW)));

        results.add(new AclBinding(
                new ResourcePattern(ResourceType.TRANSACTIONAL_ID, resource, PatternType.PREFIXED),
                new org.apache.kafka.common.acl.AccessControlEntry(
                        principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    /**
     * Convert Ns4Kafka connect ACL into Kafka ACL.
     *
     * @param acl The Ns4Kafka ACL
     * @return A Kafka ACL
     */
    private AclBinding convertConnectorAclToAclBinding(AccessControlEntry acl) {
        PatternType patternType =
                PatternType.fromString(acl.getSpec().getResourcePatternType().toString());

        ResourcePattern resourcePattern = new ResourcePattern(
                ResourceType.GROUP, "connect-" + acl.getSpec().getResource(), patternType);

        String kafkaUser = namespaceRepository
                .findByName(acl.getSpec().getGrantedTo())
                .orElseThrow()
                .getSpec()
                .getKafkaUser();

        return new AclBinding(
                resourcePattern,
                new org.apache.kafka.common.acl.AccessControlEntry(
                        USER_PRINCIPAL + kafkaUser, "*", AclOperation.READ, AclPermissionType.ALLOW));
    }

    /**
     * Convert public ACL into Kafka ACL Binding.
     *
     * @param acl The Ns4Kafka ACL
     */
    AclBinding convertPublicAcl(AccessControlEntry acl) {
        PatternType patternType =
                PatternType.fromString(acl.getSpec().getResourcePatternType().toString());

        ResourcePattern resourcePattern = new ResourcePattern(
                ResourceType.fromString(acl.getSpec().getResourceType().toString()),
                acl.getSpec().getResource(),
                patternType);

        return new AclBinding(
                resourcePattern,
                new org.apache.kafka.common.acl.AccessControlEntry(
                        USER_PRINCIPAL_PUBLIC,
                        "*",
                        AclOperation.fromString(acl.getSpec().getPermission().toString()),
                        AclPermissionType.ALLOW));
    }

    /**
     * Delete a given list of ACLs.
     *
     * @param toDelete The list of ACLs to delete
     */
    void deleteAcls(List<AclBinding> toDelete) {
        getAdminClient()
                .deleteAcls(toDelete.stream().map(AclBinding::toFilter).toList())
                .values()
                .forEach((key, value) -> {
                    try {
                        value.get(managedClusterProperties.getTimeout().getAcl().getDelete(), TimeUnit.MILLISECONDS);
                        log.info("Success deleting ACL {} on cluster {}.", key, managedClusterProperties.getName());
                    } catch (InterruptedException e) {
                        log.error("Error", e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(
                                "Error while deleting ACL {} on cluster {}.",
                                key,
                                managedClusterProperties.getName(),
                                e);
                    }
                });
    }

    /**
     * Delete a given Ns4Kafka ACL. Convert Ns4Kafka ACL into Kafka ACLs before deletion.
     *
     * @param accessControlEntry The ACL
     */
    public void deleteAcl(AccessControlEntry accessControlEntry) {
        if (managedClusterProperties.isManageAcls()) {
            List<AclBinding> results = new ArrayList<>();

            if (TOPIC_GROUP_RESOURCE_TYPES.contains(accessControlEntry.getSpec().getResourceType())) {
                results.addAll(convertAclToAclBindings(accessControlEntry));
            }

            if (accessControlEntry.getSpec().getResourceType() == CONNECT
                    && accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
                results.add(convertConnectorAclToAclBinding(accessControlEntry));
            }

            deleteAcls(results);
        } else {
            if (aclService.isPublicAcl(accessControlEntry) && managedClusterProperties.isManageRbac()) {
                deleteAcls(List.of(convertPublicAcl(accessControlEntry)));
            }
        }
    }

    /**
     * Create a given list of ACLs.
     *
     * @param toCreate The list of ACLs to create
     * @param created The ACLs successfully created
     * @param creationErrors The error message of each ACL that could not be created
     */
    private void createAcls(
            List<AclBinding> toCreate, Set<AclBinding> created, Map<AclBinding, String> creationErrors) {
        getAdminClient().createAcls(toCreate).values().forEach((key, value) -> {
            try {
                value.get(managedClusterProperties.getTimeout().getAcl().getCreate(), TimeUnit.MILLISECONDS);
                created.add(key);
                log.info("Success creating ACL {} on cluster {}.", key, managedClusterProperties.getName());
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                creationErrors.put(
                        key, Objects.toString(e.getMessage(), e.getClass().getName()));
                log.error("Error while creating ACL {} on cluster {}.", key, managedClusterProperties.getName(), e);
            }
        });
    }

    /**
     * Delete a given Kafka Streams.
     *
     * @param kafkaStream The Kafka Streams
     */
    public void deleteKafkaStreams(Namespace namespace, KafkaStream kafkaStream) {
        if (managedClusterProperties.isManageAcls()) {
            String principal = USER_PRINCIPAL + namespace.getSpec().getKafkaUser();
            List<AclBinding> results =
                    buildAclBindingsFromKafkaStream(kafkaStream, principal).toList();
            deleteAcls(results);
        }
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
