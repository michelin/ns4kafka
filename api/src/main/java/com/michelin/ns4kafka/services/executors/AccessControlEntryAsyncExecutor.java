package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.KafkaStream;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.StreamService;

import io.micronaut.context.annotation.EachBean;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class AccessControlEntryAsyncExecutor {

    private KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryService accessControlEntryService;
    @Inject
    StreamService streamService;

    public AccessControlEntryAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
    }

    public void run() {
        if (this.kafkaAsyncExecutorConfig.isManageAcls()) {
            synchronizeACLs();
        }
    }

    private Admin getAdminClient() {
        return kafkaAsyncExecutorConfig.getAdminClient();
    }

    private void synchronizeACLs() {

        log.debug("Starting ACL collection for cluster {}", kafkaAsyncExecutorConfig.getName());
        try {
            // List ACLs from broker
            List<AclBinding> brokerACLs = collectBrokerACLs(true);
            List<AclBinding> ns4kafkaACLs = collectNs4KafkaACLs();

            List<AclBinding> toCreate = ns4kafkaACLs.stream()
                    .filter(aclBinding -> !brokerACLs.contains(aclBinding))
                    .collect(Collectors.toList());
            List<AclBinding> toDelete = brokerACLs.stream()
                    .filter(aclBinding -> !ns4kafkaACLs.contains(aclBinding))
                    .collect(Collectors.toList());

            if (log.isDebugEnabled()) {
                brokerACLs.stream()
                        .filter(aclBinding -> ns4kafkaACLs.contains(aclBinding))
                        .forEach(aclBinding -> log.debug("Found in both : " + aclBinding.toString()));
                toCreate.forEach(aclBinding -> log.debug("to create : " + aclBinding.toString()));
                toDelete.forEach(aclBinding -> log.debug("to delete : " + aclBinding.toString()));
            }

            // Execute toAdd list BEFORE toDelete list to avoid breaking ACL on connected user
            // such as deleting <LITERAL "toto.titi"> only to add one second later <PREFIX "toto.">
            createACLs(toCreate);
            deleteACLs(toDelete);

        } catch (KafkaStoreException | ExecutionException | TimeoutException e) {
            log.error("Error", e);
        } catch (InterruptedException e) {
            log.error("Error", e);
            Thread.currentThread().interrupt();
        }
    }

    private List<AclBinding> collectNs4KafkaACLs() {
        // List ACLs from ns4kafka Repository and apply the following rules
        // Whenever the Permission is OWNER, create 2 entries (one READ and one WRITE)
        // This is necessary to translate ns4kafka grouped AccessControlEntry (OWNER, WRITE, READ)
        // into Kafka Atomic ACL (READ and WRITE)

        // TODO this returns only the default user with ACL "inherited" from the namespace
        //   at some point we want to manage multiple users within a namespace, each having their own ACLs.

        List<Namespace> namespaces = namespaceRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName());
        List<AclBinding> ns4kafkaACLs = Stream.concat(
            namespaces.stream()
                .flatMap(namespace -> accessControlEntryService.findAllGrantedToNamespace(namespace)
                        .stream()
                        .filter(accessControlEntry -> accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC ||
                                accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.GROUP)
                        //1-N ACE to List<AclBinding>
                        .flatMap(accessControlEntry ->
                                buildAclBindingsFromAccessControlEntry(accessControlEntry, namespace.getSpec().getKafkaUser()).stream()))
            , namespaces.stream()
                .flatMap(namespace -> streamService.findAllForNamespace(namespace)
                        .stream()
                        .flatMap(kafkaStream ->
                                buildAclBindingsFromKafkaStream(kafkaStream, namespace.getSpec().getKafkaUser()).stream()))
            ).collect(Collectors.toList());

        if (log.isDebugEnabled()) {
            log.debug("ACLs found on ns4kafka : " + ns4kafkaACLs.size());
            ns4kafkaACLs.forEach(aclBinding -> log.debug(aclBinding.toString()));
        }
        return ns4kafkaACLs;
    }

    private List<AclBinding> collectBrokerACLs(boolean managedUsersOnly) throws ExecutionException, InterruptedException, TimeoutException {
        //TODO soon : manage IDEMPOTENT_WRITE on CLUSTER 'kafka-cluster'
        //TODO eventually : manage DELEGATION_TOKEN and TRANSACTIONAL_ID
        //TODO eventually : manage host ?
        //TODO never ever : manage CREATE and DELETE Topics (managed by ns4kafka !)

        List<ResourceType> validResourceTypes = List.of(ResourceType.TOPIC, ResourceType.GROUP);

        // keep only TOPIC and GROUP Resource Types
        List<AclBinding> userACLs = getAdminClient()
                .describeAcls(AclBindingFilter.ANY)
                .values().get(10, TimeUnit.SECONDS)
                .stream()
                .filter(aclBinding -> validResourceTypes.contains(aclBinding.pattern().resourceType()))
                .collect(Collectors.toList());

        log.debug("ACLs found on Broker (total) : {}", userACLs.size());
        if (log.isTraceEnabled()) {

            userACLs.forEach(aclBinding -> {
                log.trace(aclBinding.toString());
            });
        }
        //TODO add parameter to cluster configuration to scope ALL users vs "namespace" managed users
        // as of now, this will prevent deletion of ACLs for users not in ns4kafka scope
        if (managedUsersOnly) {
            // we first collect the list of Users managed in ns4kafka
            List<String> managedUsers = namespaceRepository.findAllForCluster(kafkaAsyncExecutorConfig.getName())
                    .stream()
                    //TODO managed user list should include not only "defaultKafkaUser" (MVP35)
                    //1-N Namespace to KafkaUser
                    .flatMap(namespace -> List.of("User:" + namespace.getSpec().getKafkaUser()).stream())
                    .collect(Collectors.toList());
            // And then filter out the AclBinding to retain only those matching.
            userACLs = userACLs.stream()
                    .filter(aclBinding -> managedUsers.contains(aclBinding.entry().principal()))
                    .collect(Collectors.toList());
            log.debug("ACLs found on Broker (managed scope) : {}", userACLs.size());
        }

        if (log.isDebugEnabled()) {
            userACLs.forEach(aclBinding -> {
                log.debug(aclBinding.toString());
            });
        }

        return userACLs;
    }

    private List<AclBinding> buildAclBindingsFromAccessControlEntry(AccessControlEntry accessControlEntry, String kafkaUser) {
        //convert pattern, convert resource type from NS4Kafka to org.apache.kafka.common types
        PatternType patternType = PatternType.fromString(accessControlEntry.getSpec().getResourcePatternType().toString());
        ResourceType resourceType = ResourceType.fromString(accessControlEntry.getSpec().getResourceType().toString());
        ResourcePattern resourcePattern = new ResourcePattern(resourceType,
                accessControlEntry.getSpec().getResource(),
                patternType);

        //generate the required AclOperation based on ResourceType
        List<AclOperation> targetAclOperations = null;
        if (accessControlEntry.getSpec().getPermission() == AccessControlEntry.Permission.OWNER) {
            targetAclOperations = computeAclOperationForOwner(resourceType);
        } else {
            //should be READ or WRITE
            targetAclOperations = List.of(AclOperation.fromString(accessControlEntry.getSpec().getPermission().toString()));
        }
        return targetAclOperations.stream().map(aclOperation ->
                new AclBinding(resourcePattern, new org.apache.kafka.common.acl.AccessControlEntry("User:" + kafkaUser, "*", aclOperation, AclPermissionType.ALLOW)))
                .collect(Collectors.toList());
    }

    private List<AclBinding> buildAclBindingsFromKafkaStream(KafkaStream stream, String kafkaUser) {
        return List.of(
                // CREATE and DELETE on Stream Topics
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, stream.getMetadata().getName(), PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry("User:" + kafkaUser, "*", AclOperation.CREATE, AclPermissionType.ALLOW)
                ),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, stream.getMetadata().getName(), PatternType.PREFIXED),
                        new org.apache.kafka.common.acl.AccessControlEntry("User:" + kafkaUser, "*", AclOperation.DELETE, AclPermissionType.ALLOW)
                )
        );
    }

    private List<AclOperation> computeAclOperationForOwner(ResourceType resourceType) {
        switch (resourceType) {
            case TOPIC:
                return List.of(AclOperation.WRITE, AclOperation.READ);
            case GROUP:
                return List.of(AclOperation.READ);
            case CLUSTER:
            case TRANSACTIONAL_ID:
            case DELEGATION_TOKEN:
            default:
                throw new IllegalArgumentException("Not implemented yet :" + resourceType.toString());
        }
    }

    private void deleteACLs(List<AclBinding> toDelete) {
        getAdminClient()
                .deleteAcls(toDelete.stream()
                        .map(aclBinding -> aclBinding.toFilter())
                        .collect(Collectors.toList()))
                .values().entrySet()
                .stream()
                .forEach(mapEntry -> {
                    try {
                        mapEntry.getValue().get(10, TimeUnit.SECONDS);
                        log.info("Success deleting ACL {} on {}", mapEntry.getKey(), this.kafkaAsyncExecutorConfig.getName());
                    } catch (InterruptedException e) {
                        log.error("Error", e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(String.format("Error while deleting ACL %s on %s", mapEntry.getKey(), this.kafkaAsyncExecutorConfig.getName()), e);
                    }
                });
    }

    private void createACLs(List<AclBinding> toCreate) {
        getAdminClient().createAcls(toCreate)
                .values()
                .entrySet()
                .stream()
                .forEach(mapEntry -> {
                    try {
                        mapEntry.getValue().get(10, TimeUnit.SECONDS);
                        log.info("Success creating ACL {} on {}", mapEntry.getKey(), this.kafkaAsyncExecutorConfig.getName());
                    } catch (InterruptedException e) {
                        log.error("Error", e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(String.format("Error while creating ACL %s on %s", mapEntry.getKey(), this.kafkaAsyncExecutorConfig.getName()), e);
                    }
                });
    }
}
