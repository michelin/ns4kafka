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
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.service.AclService;
import com.michelin.ns4kafka.service.StreamService;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
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
    private final ManagedClusterProperties managedClusterProperties;
    private final AclService aclService;
    private final StreamService streamService;
    private final NamespaceRepository namespaceRepository;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param aclService The ACL service
     * @param streamService The stream service
     * @param namespaceRepository The namespace repository
     */
    public AccessControlEntryAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            AclService aclService,
            StreamService streamService,
            NamespaceRepository namespaceRepository) {
        this.managedClusterProperties = managedClusterProperties;
        this.aclService = aclService;
        this.streamService = streamService;
        this.namespaceRepository = namespaceRepository;
    }

    /** Run the ACLs synchronization. */
    public void run() {
        if (this.managedClusterProperties.isManageAcls()) {
            synchronizeAcls();
        }
    }

    /** Start the ACLs synchronization. */
    private void synchronizeAcls() {
        log.debug("Starting ACL collection for cluster {}", managedClusterProperties.getName());

        try {
            // List ACLs from broker
            Set<AclBinding> brokerAcls = collectBrokerAcls(true);

            // List ACLs from NS4Kafka
            Set<AclBinding> ns4kafkaAcls = collectNs4KafkaAcls();

            // Add ACLs before delete to avoid breaking ACL
            // such as deleting <LITERAL "toto.titi"> only to add one second later <PREFIX "toto.">
            List<AclBinding> toCreate = ns4kafkaAcls.stream()
                    .filter(aclBinding -> !brokerAcls.contains(aclBinding))
                    .toList();

            if (!toCreate.isEmpty()) {
                log.debug(
                        "ACL(s) to create: {}",
                        String.join(
                                ",", toCreate.stream().map(AclBinding::toString).toList()));

                createAcls(toCreate);
            }

            if (managedClusterProperties.isDropUnsyncAcls()) {
                List<AclBinding> toDelete = brokerAcls.stream()
                        .filter(aclBinding -> !ns4kafkaAcls.contains(aclBinding))
                        .toList();

                if (!toDelete.isEmpty()) {
                    log.debug(
                            "ACL(s) to delete: {}",
                            String.join(
                                    ",",
                                    toDelete.stream().map(AclBinding::toString).toList()));

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
        List<AccessControlEntry> acls = aclService.findAllForCluster(managedClusterProperties.getName());

        // Converts topic and group Ns4Kafka ACLs to topic & group & transactional AclBindings
        Stream<AclBinding> aclBindingsFromAcls = acls.stream()
                .filter(acl -> List.of(TOPIC, GROUP).contains(acl.getSpec().getResourceType()))
                .flatMap(acl -> convertAclToAclBindings(acl).stream())
                .distinct();

        // Converts connector ACLs to group AclBindings (connect-)
        Stream<AclBinding> aclBindingsFromConnect = acls.stream()
                .filter(acl -> acl.getSpec().getResourceType() == CONNECT
                        && acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER)
                .map(this::convertConnectorAclToAclBinding);

        // Converts KafkaStream resources to topic (CREATE/DELETE) AclBindings
        // Looping over namespaces because some Kafka Streams might have a non-existing namespace
        Stream<AclBinding> aclBindingFromKstream =
                namespaceRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                        .flatMap(namespace -> {
                            String kafkaUser = namespace.getSpec().getKafkaUser();
                            return streamService.findAllForNamespace(namespace).stream()
                                    .flatMap(kafkaStream -> buildAclBindingsFromKafkaStream(kafkaStream, kafkaUser));
                        });

        Set<AclBinding> ns4kafkaAcls = Stream.of(aclBindingsFromAcls, aclBindingsFromConnect, aclBindingFromKstream)
                .flatMap(Function.identity())
                .collect(Collectors.toSet());

        if (!ns4kafkaAcls.isEmpty()) {
            // Use Fluent logging API + lambda so the ACLs list is only computed when at trace log level
            log.atTrace()
                    .addArgument(() -> String.join(
                            ",", ns4kafkaAcls.stream().map(AclBinding::toString).toList()))
                    .log("ACL(s) found in Ns4Kafka: {}");
        }

        return ns4kafkaAcls;
    }

    /**
     * Collect the ACLs from broker.
     *
     * @param managedUsersOnly Only retrieve ACLs from Kafka user managed by Ns4Kafka or not?
     * @return A set of ACLs
     * @throws ExecutionException Any execution exception during ACLs description
     * @throws InterruptedException Any interrupted exception during ACLs description
     * @throws TimeoutException Any timeout exception during ACLs description
     */
    private Set<AclBinding> collectBrokerAcls(boolean managedUsersOnly)
            throws ExecutionException, InterruptedException, TimeoutException {
        List<ResourceType> validResourceTypes = List.of(
                org.apache.kafka.common.resource.ResourceType.TOPIC,
                org.apache.kafka.common.resource.ResourceType.GROUP,
                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID);

        AccessControlEntryFilter accessControlEntryFilter = new AccessControlEntryFilter(
                managedClusterProperties.getProvider().equals(ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD)
                        ? "UserV2:*"
                        : null,
                null,
                AclOperation.ANY,
                AclPermissionType.ANY);
        AclBindingFilter aclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY, accessControlEntryFilter);

        Set<AclBinding> userAcls = getAdminClient()
                .describeAcls(aclBindingFilter)
                .values()
                .get(managedClusterProperties.getTimeout().getAcl().getDescribe(), TimeUnit.MILLISECONDS)
                .stream()
                .filter(aclBinding ->
                        validResourceTypes.contains(aclBinding.pattern().resourceType()))
                .collect(Collectors.toSet());

        if (managedUsersOnly) {
            // Collect the list of users managed in Ns4Kafka
            List<String> managedUsers = new ArrayList<>();
            managedUsers.add(USER_PRINCIPAL + PUBLIC_GRANTED_TO);
            managedUsers.addAll(namespaceRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                    .flatMap(namespace ->
                            Stream.of(USER_PRINCIPAL + namespace.getSpec().getKafkaUser()))
                    .toList());

            // Filter out the ACLs to retain only those matching
            userAcls = userAcls.stream()
                    .filter(aclBinding ->
                            managedUsers.contains(aclBinding.entry().principal()))
                    .collect(Collectors.toSet());
        }

        // Use logging guard so the ACLs list is only computed when at trace log level
        if (log.isTraceEnabled() && !userAcls.isEmpty()) {
            log.trace(
                    "ACL(s) found in broker{}: {}",
                    managedUsersOnly ? " (managed users scope)" : "",
                    String.join(",", userAcls.stream().map(AclBinding::toString).toList()));
        }

        return userAcls;
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
        List<AclOperation> aclOperations;
        if (acl.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
            aclOperations = computeAclOperationForOwner(resourceType);
        } else {
            aclOperations = List.of(
                    AclOperation.fromString(acl.getSpec().getPermission().toString()));
        }

        String kafkaUser;
        Stream<AclBinding> transactionalIdAcls = Stream.of();

        if (acl.getSpec().getGrantedTo().equals(PUBLIC_GRANTED_TO)) {
            kafkaUser = PUBLIC_GRANTED_TO;
        } else {
            Namespace namespace =
                    namespaceRepository.findByName(acl.getSpec().getGrantedTo()).orElseThrow();
            kafkaUser = namespace.getSpec().getKafkaUser();

            // Generate KafkaStream ACLs and transactions ACLs for GROUP ACL
            if (GROUP.equals(acl.getSpec().getResourceType())) {
                if (namespace.getSpec().isTransactionsEnabled()) {
                    transactionalIdAcls = buildEOSConnectorAclBindingsFromGroupAcl(
                            acl, namespace.getSpec().getKafkaUser());
                } else {
                    if (streamService.hasKafkaStream(namespace)) {
                        transactionalIdAcls = buildKafkaStreamAclBindingsFromGroupAcl(
                                acl, namespace.getSpec().getKafkaUser());
                    }
                }
            }
        }

        return Stream.concat(
                        transactionalIdAcls,
                        aclOperations.stream()
                                .map(aclOperation -> new AclBinding(
                                        resourcePattern,
                                        new org.apache.kafka.common.acl.AccessControlEntry(
                                                USER_PRINCIPAL + kafkaUser,
                                                "*",
                                                aclOperation,
                                                AclPermissionType.ALLOW))))
                .toList();
    }

    /**
     * Convert Kafka Stream to ACL Bindings.
     *
     * @param stream The Kafka Stream resource
     * @param kafkaUser The kafka User
     * @return A stream of Kafka ACLs
     * @see <a
     *     href="https://docs.confluent.io/platform/current/streams/developer-guide/security.html#required-acl-setting-for-secure-ak-clusters">Required
     *     ACL setting for secure Kafka clusters</a>
     */
    private Stream<AclBinding> buildAclBindingsFromKafkaStream(KafkaStream stream, String kafkaUser) {
        return Stream.of(
                // Kafka Stream needs to create & delete changelog/repartition topics with the application id as prefix
                // PREFIXED ACLs to cover all the namespace Kafka Streams.
                new AclBinding(
                        new ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TOPIC,
                                stream.getMetadata().getName(),
                                PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                USER_PRINCIPAL + kafkaUser, "*", AclOperation.CREATE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TOPIC,
                                stream.getMetadata().getName(),
                                PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                USER_PRINCIPAL + kafkaUser, "*", AclOperation.DELETE, AclPermissionType.ALLOW)));
    }

    /**
     * Convert group ACL to KafkaStream Kafka ACL.
     *
     * @param acl The Ns4kafka group ACL
     * @param kafkaUser The kafka User
     * @return A stream of Kafka ACLs
     * @see <a
     *     href="https://docs.confluent.io/platform/current/streams/developer-guide/security.html#required-acl-setting-for-secure-ak-clusters">Required
     *     ACL setting for secure Kafka clusters</a>
     */
    private Stream<AclBinding> buildKafkaStreamAclBindingsFromGroupAcl(AccessControlEntry acl, String kafkaUser) {
        return Stream.of(
                // PREFIXED ACLs to cover all Kafka Streams & EOS connectors.
                new AclBinding(
                        new ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                                acl.getSpec().getResource(),
                                PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                USER_PRINCIPAL + kafkaUser, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                                acl.getSpec().getResource(),
                                PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                USER_PRINCIPAL + kafkaUser, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    /**
     * Build Transactional ID Kafka ACLs from the given Ns4Kafka Group ACL, to allow transactions.
     *
     * @param acl The Ns4kafka group ACL
     * @param kafkaUser The kafka user
     * @return A stream of Kafka ACLs
     * @see <a
     *     href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=153816406#KIP618:ExactlyOnceSupportforSourceConnectors-Workerprincipalpermissions">ExactlyOnceSupportforSourceConnectors</a>
     */
    private Stream<AclBinding> buildEOSConnectorAclBindingsFromGroupAcl(AccessControlEntry acl, String kafkaUser) {
        return Stream.of(
                // EOS connectors need "write" and "describe" on "connect-cluster-${groupId}".
                new AclBinding(
                        new ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                                "connect-cluster-" + acl.getSpec().getResource(),
                                PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                USER_PRINCIPAL + kafkaUser, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                                "connect-cluster-" + acl.getSpec().getResource(),
                                PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                USER_PRINCIPAL + kafkaUser, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),

                // EOS connectors need "write" and "describe" on "${groupId}-${connector}-${taskId}".
                // PREFIXED ACLs to cover all Kafka Streams & EOS connectors.
                new AclBinding(
                        new ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                                acl.getSpec().getResource(),
                                PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                USER_PRINCIPAL + kafkaUser, "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID,
                                acl.getSpec().getResource(),
                                PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry(
                                USER_PRINCIPAL + kafkaUser, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
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
                org.apache.kafka.common.resource.ResourceType.GROUP,
                "connect-" + acl.getSpec().getResource(),
                patternType);

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
     * Get ACL operations from given resource type.
     *
     * @param resourceType The resource type
     * @return A list of ACL operations
     */
    private List<AclOperation> computeAclOperationForOwner(ResourceType resourceType) {
        return switch (resourceType) {
            case TOPIC -> List.of(AclOperation.WRITE, AclOperation.READ, AclOperation.DESCRIBE_CONFIGS);
            case GROUP -> List.of(AclOperation.READ);
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
                .deleteAcls(toDelete.stream().map(AclBinding::toFilter).toList())
                .values()
                .forEach((key, value) -> {
                    try {
                        value.get(managedClusterProperties.getTimeout().getAcl().getDelete(), TimeUnit.MILLISECONDS);
                        log.info("Success deleting ACL {} on {}", key, managedClusterProperties.getName());
                    } catch (InterruptedException e) {
                        log.error("Error", e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error("Error while deleting ACL {} on {}", key, managedClusterProperties.getName(), e);
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
            String kafkaUser = namespace.getSpec().getKafkaUser();
            List<AclBinding> results = new ArrayList<>(
                    buildAclBindingsFromKafkaStream(kafkaStream, kafkaUser).toList());
            deleteAcls(results);
        }
    }

    /**
     * Delete a given Ns4Kafka ACL. Convert Ns4Kafka ACL into Kafka ACLs before deletion.
     *
     * @param accessControlEntry The ACL
     */
    public void deleteAcl(AccessControlEntry accessControlEntry) {
        if (managedClusterProperties.isManageAcls()) {
            List<AclBinding> results = new ArrayList<>();

            if (List.of(TOPIC, GROUP).contains(accessControlEntry.getSpec().getResourceType())) {
                results.addAll(convertAclToAclBindings(accessControlEntry));
            }

            if (accessControlEntry.getSpec().getResourceType() == CONNECT
                    && accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
                results.add(convertConnectorAclToAclBinding(accessControlEntry));
            }

            deleteAcls(results);
        }
    }

    /**
     * Create a given list of ACLs.
     *
     * @param toCreate The list of ACLs to create
     */
    private void createAcls(List<AclBinding> toCreate) {
        getAdminClient().createAcls(toCreate).values().forEach((key, value) -> {
            try {
                value.get(managedClusterProperties.getTimeout().getAcl().getCreate(), TimeUnit.MILLISECONDS);
                log.info("Success creating ACL {} on {}", key, managedClusterProperties.getName());
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error while creating ACL {} on {}", key, managedClusterProperties.getName(), e);
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
