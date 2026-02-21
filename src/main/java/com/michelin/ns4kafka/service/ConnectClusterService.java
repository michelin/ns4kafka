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
package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConnectClusterEncryptionConfig;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConnectClusterNameAlreadyExistGlobally;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConnectClusterNoEncryptionConfig;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConnectClusterNotHealthy;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNotFound;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.model.connect.cluster.VaultResponse;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.property.Ns4KafkaProperties;
import com.michelin.ns4kafka.repository.ConnectClusterRepository;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient;
import com.michelin.ns4kafka.service.client.connect.KafkaConnectClient.KafkaConnectHttpConfig;
import com.michelin.ns4kafka.util.EncryptionUtils;
import com.michelin.ns4kafka.util.RegexUtils;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Service to manage Kafka Connect clusters. */
@Slf4j
@Singleton
public class ConnectClusterService {
    /** The default format string for aes 256 conversion. */
    private static final String DEFAULT_FORMAT = "${aes256:%s}";

    private static final String WILDCARD_SECRET = "*****";

    private final KafkaConnectClient kafkaConnectClient;
    private final AclService aclService;
    private final ConnectClusterRepository connectClusterRepository;
    private final List<ManagedClusterProperties> managedClusterProperties;
    private final Ns4KafkaProperties ns4KafkaProperties;

    @Getter
    @Setter
    private Set<String> healthyConnectClusters = new HashSet<>();

    /**
     * Constructor.
     *
     * @param kafkaConnectClient The Kafka Connect client
     * @param aclService The ACL service
     * @param connectClusterRepository The Connect cluster repository
     * @param managedClusterProperties The list of managed cluster properties
     * @param ns4KafkaProperties The NS4Kafka properties
     */
    public ConnectClusterService(
            KafkaConnectClient kafkaConnectClient,
            AclService aclService,
            ConnectClusterRepository connectClusterRepository,
            List<ManagedClusterProperties> managedClusterProperties,
            Ns4KafkaProperties ns4KafkaProperties) {
        this.kafkaConnectClient = kafkaConnectClient;
        this.aclService = aclService;
        this.connectClusterRepository = connectClusterRepository;
        this.managedClusterProperties = managedClusterProperties;
        this.ns4KafkaProperties = ns4KafkaProperties;
    }

    /**
     * Find all self deployed Connect clusters.
     *
     * @param all Include hard-declared Connect clusters
     * @param status Include status information
     * @return A list of Connect clusters
     */
    public Flux<ConnectCluster> findAll(boolean all, boolean status) {
        List<ConnectCluster> results = connectClusterRepository.findAll();

        if (all) {
            results.addAll(managedClusterProperties.stream()
                    .filter(cluster -> cluster.getConnects() != null)
                    .flatMap(config -> config.getConnects().entrySet().stream().map(entry -> ConnectCluster.builder()
                            .metadata(Metadata.builder()
                                    .name(entry.getKey())
                                    .cluster(config.getName())
                                    .build())
                            .spec(ConnectCluster.ConnectClusterSpec.builder()
                                    .url(entry.getValue().getUrl())
                                    .username(entry.getValue().getBasicAuthUsername())
                                    .password(entry.getValue().getBasicAuthPassword())
                                    .build())
                            .build()))
                    .toList());
        }

        return status
                ? Flux.fromIterable(results).flatMap(connectCluster -> kafkaConnectClient
                        .version(
                                connectCluster.getMetadata().getCluster(),
                                connectCluster.getMetadata().getName())
                        .doOnError(error -> {
                            connectCluster.getSpec().setStatus(ConnectCluster.Status.IDLE);
                            connectCluster.getSpec().setStatusMessage(error.getMessage());
                        })
                        .doOnSuccess(_ -> {
                            connectCluster.getSpec().setStatus(ConnectCluster.Status.HEALTHY);
                            connectCluster.getSpec().setStatusMessage(null);
                        })
                        .map(_ -> connectCluster)
                        .onErrorReturn(connectCluster))
                : Flux.fromIterable(results);
    }

    /**
     * Find all self deployed Connect clusters of a given namespace, with a given list of permissions.
     *
     * @param namespace The namespace
     * @param permissions The list of permission to filter on
     * @return A list of Connect clusters
     */
    public List<ConnectCluster> findAllForNamespaceByPermissions(
            Namespace namespace, List<AccessControlEntry.Permission> permissions) {
        List<AccessControlEntry> acls = aclService.findAllGrantedToNamespace(namespace).stream()
                .filter(acl -> permissions.contains(acl.getSpec().getPermission())
                        && acl.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                .toList();

        return connectClusterRepository
                .findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(connectCluster -> aclService.isResourceCoveredByAcls(
                        acls, connectCluster.getMetadata().getName()))
                .toList();
    }

    /**
     * Find all self deployed Connect clusters whose namespace is owner.
     *
     * @param namespace The namespace
     * @return A list of Connect clusters
     */
    public List<ConnectCluster> findAllForNamespaceWithOwnerPermission(Namespace namespace) {
        return findAllForNamespaceByPermissions(namespace, List.of(AccessControlEntry.Permission.OWNER)).stream()
                .toList();
    }

    /**
     * Find all self deployed Connect clusters whose namespace is owner, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @return The list of owned Connect cluster
     */
    public List<ConnectCluster> findByWildcardNameWithOwnerPermission(Namespace namespace, String name) {
        List<String> nameFilterPatterns = RegexUtils.convertWildcardStringsToRegex(List.of(name));
        return findAllForNamespaceWithOwnerPermission(namespace).stream()
                .filter(cc ->
                        RegexUtils.isResourceCoveredByRegex(cc.getMetadata().getName(), nameFilterPatterns))
                .map(this::buildConnectClusterWithDecryptedInformation)
                .toList();
    }

    /**
     * Find a self deployed Connect cluster by namespace and name with owner rights.
     *
     * @param namespace The namespace
     * @param connectClusterName The connect worker name
     * @return An optional connect worker
     */
    public Optional<ConnectCluster> findByNameWithOwnerPermission(Namespace namespace, String connectClusterName) {
        return findAllForNamespaceWithOwnerPermission(namespace).stream()
                .filter(cc -> cc.getMetadata().getName().equals(connectClusterName))
                .map(this::buildConnectClusterWithDecryptedInformation)
                .findFirst();
    }

    /**
     * Find all self deployed Connect clusters whose namespace has write access.
     *
     * @param namespace The namespace
     * @return The list of Connect cluster with write access
     */
    public List<ConnectCluster> findAllForNamespaceWithWritePermission(Namespace namespace) {
        return Stream.concat(
                        findByWildcardNameWithOwnerPermission(namespace, "*").stream(),
                        findAllForNamespaceByPermissions(namespace, List.of(AccessControlEntry.Permission.WRITE))
                                .stream()
                                .map(connectCluster -> ConnectCluster.builder()
                                        .metadata(connectCluster.getMetadata())
                                        .spec(ConnectCluster.ConnectClusterSpec.builder()
                                                .url(connectCluster.getSpec().getUrl())
                                                .username(
                                                        connectCluster.getSpec().getUsername())
                                                .password(WILDCARD_SECRET)
                                                .aes256Key(WILDCARD_SECRET)
                                                .aes256Salt(WILDCARD_SECRET)
                                                .aes256Format(
                                                        connectCluster.getSpec().getAes256Format())
                                                .build())
                                        .build()))
                .toList();
    }

    /**
     * Create a given connect worker.
     *
     * @param connectCluster The connect worker
     * @return The created connect worker
     */
    public ConnectCluster create(ConnectCluster connectCluster) {
        if (StringUtils.hasText(connectCluster.getSpec().getPassword())) {
            connectCluster
                    .getSpec()
                    .setPassword(EncryptionUtils.encryptAes256Gcm(
                            connectCluster.getSpec().getPassword(),
                            ns4KafkaProperties.getSecurity().getAes256EncryptionKey()));
        }

        // Encrypt aes256 key if present
        if (StringUtils.hasText(connectCluster.getSpec().getAes256Key())) {
            connectCluster
                    .getSpec()
                    .setAes256Key(EncryptionUtils.encryptAes256Gcm(
                            connectCluster.getSpec().getAes256Key(),
                            ns4KafkaProperties.getSecurity().getAes256EncryptionKey()));
        }

        // Encrypt aes256 salt if present
        if (StringUtils.hasText(connectCluster.getSpec().getAes256Salt())) {
            connectCluster
                    .getSpec()
                    .setAes256Salt(EncryptionUtils.encryptAes256Gcm(
                            connectCluster.getSpec().getAes256Salt(),
                            ns4KafkaProperties.getSecurity().getAes256EncryptionKey()));
        }

        return connectClusterRepository.create(connectCluster);
    }

    /**
     * Validate the given connect worker configuration for creation.
     *
     * @param connectCluster The connect worker to validate
     * @return A list of validation errors
     */
    public Mono<List<String>> validateConnectClusterCreation(ConnectCluster connectCluster) {
        List<String> errors = new ArrayList<>();

        if (managedClusterProperties.stream()
                .filter(cluster -> cluster.getConnects() != null)
                .anyMatch(cluster -> cluster.getConnects().entrySet().stream().anyMatch(entry -> entry.getKey()
                        .equals(connectCluster.getMetadata().getName())))) {
            errors.add(invalidConnectClusterNameAlreadyExistGlobally(
                    connectCluster.getMetadata().getName()));
        }

        return kafkaConnectClient
                .version(KafkaConnectHttpConfig.builder()
                        .username(connectCluster.getSpec().getUsername())
                        .password(connectCluster.getSpec().getPassword())
                        .url(connectCluster.getSpec().getUrl())
                        .build())
                .doOnError(error -> errors.add(
                        invalidConnectClusterNotHealthy(connectCluster.getSpec().getUrl(), error.getMessage())))
                .doOnEach(signal -> {
                    // If the key or salt is defined, but one of them is missing
                    if ((signal.isOnError() || signal.isOnNext())
                            && (StringUtils.hasText(connectCluster.getSpec().getAes256Key())
                                    ^ StringUtils.hasText(
                                            connectCluster.getSpec().getAes256Salt()))) {
                        errors.add(invalidConnectClusterEncryptionConfig());
                    }
                })
                .map(_ -> errors)
                .onErrorReturn(errors);
    }

    /**
     * Validate the given connect worker has configuration for vault.
     *
     * @param namespace The namespace
     * @param connectClusterName The Kafka connect worker to validate
     * @return A list of validation errors
     */
    public List<String> validateConnectClusterVault(final Namespace namespace, final String connectClusterName) {
        final List<String> errors = new ArrayList<>();

        Optional<ConnectCluster> connectClusters = findAllForNamespaceWithWritePermission(namespace).stream()
                .filter(connectCluster -> connectCluster.getMetadata().getName().equals(connectClusterName))
                .findFirst();

        if (connectClusters.isEmpty()) {
            errors.add(invalidNotFound(connectClusterName));
            return errors;
        }

        if (!StringUtils.hasText(connectClusters.get().getSpec().getAes256Key())
                || !StringUtils.hasText(connectClusters.get().getSpec().getAes256Salt())) {
            errors.add(invalidConnectClusterNoEncryptionConfig());
            return errors;
        }

        return errors;
    }

    /**
     * Delete a given Connect cluster.
     *
     * @param connectCluster The Connect cluster
     */
    public void delete(ConnectCluster connectCluster) {
        connectClusterRepository.delete(connectCluster);
    }

    /**
     * Is given namespace owner of the given connect worker.
     *
     * @param namespace The namespace
     * @param connectCluster The Kafka connect cluster
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfConnectCluster(Namespace namespace, String connectCluster) {
        return aclService.isNamespaceOwnerOfResource(
                namespace.getMetadata().getName(), AccessControlEntry.ResourceType.CONNECT_CLUSTER, connectCluster);
    }

    /**
     * Vault a password for a specific namespace and a kafka connect cluster.
     *
     * @param namespace The namespace that need an encrypted password.
     * @param connectCluster The kafka connect cluster for which to encrypt the password.
     * @param passwords The passwords list to encrypt.
     * @return The encrypted password.
     */
    public List<VaultResponse> vaultPassword(
            final Namespace namespace, final String connectCluster, final List<String> passwords) {
        final Optional<ConnectCluster> kafkaConnect =
                findAllForNamespaceByPermissions(
                                namespace,
                                List.of(AccessControlEntry.Permission.OWNER, AccessControlEntry.Permission.WRITE))
                        .stream()
                        .filter(cc -> cc.getMetadata().getName().equals(connectCluster)
                                && StringUtils.hasText(cc.getSpec().getAes256Key())
                                && StringUtils.hasText(cc.getSpec().getAes256Salt()))
                        .findFirst();

        if (kafkaConnect.isEmpty()) {
            return passwords.stream()
                    .map(password -> VaultResponse.builder()
                            .spec(VaultResponse.VaultResponseSpec.builder()
                                    .clearText(password)
                                    .encrypted(password)
                                    .build())
                            .build())
                    .toList();
        }

        final String aes256Key = EncryptionUtils.decryptAes256Gcm(
                kafkaConnect.get().getSpec().getAes256Key(),
                ns4KafkaProperties.getSecurity().getAes256EncryptionKey());
        final String aes256Salt = EncryptionUtils.decryptAes256Gcm(
                kafkaConnect.get().getSpec().getAes256Salt(),
                ns4KafkaProperties.getSecurity().getAes256EncryptionKey());
        final String aes256Format =
                StringUtils.hasText(kafkaConnect.get().getSpec().getAes256Format())
                        ? kafkaConnect.get().getSpec().getAes256Format()
                        : DEFAULT_FORMAT;

        return passwords.stream()
                .map(password -> VaultResponse.builder()
                        .spec(VaultResponse.VaultResponseSpec.builder()
                                .clearText(password)
                                .encrypted(aes256Format.formatted(
                                        EncryptionUtils.encryptAesWithPrefix(password, aes256Key, aes256Salt)))
                                .build())
                        .build())
                .toList();
    }

    /**
     * Build the same connect cluster object with decrypted information.
     *
     * @param connectCluster The kafka connect cluster for which to decrypt the information.
     * @return The connect cluster with decrypted information.
     */
    public ConnectCluster buildConnectClusterWithDecryptedInformation(ConnectCluster connectCluster) {
        var builder = ConnectCluster.ConnectClusterSpec.builder()
                .url(connectCluster.getSpec().getUrl())
                .username(connectCluster.getSpec().getUsername())
                .password(EncryptionUtils.decryptAes256Gcm(
                        connectCluster.getSpec().getPassword(),
                        ns4KafkaProperties.getSecurity().getAes256EncryptionKey()))
                .aes256Key(EncryptionUtils.decryptAes256Gcm(
                        connectCluster.getSpec().getAes256Key(),
                        ns4KafkaProperties.getSecurity().getAes256EncryptionKey()))
                .aes256Salt(EncryptionUtils.decryptAes256Gcm(
                        connectCluster.getSpec().getAes256Salt(),
                        ns4KafkaProperties.getSecurity().getAes256EncryptionKey()))
                .aes256Format(connectCluster.getSpec().getAes256Format())
                .status(
                        healthyConnectClusters.contains(
                                        connectCluster.getMetadata().getName())
                                ? ConnectCluster.Status.HEALTHY
                                : ConnectCluster.Status.IDLE);

        return ConnectCluster.builder()
                .metadata(connectCluster.getMetadata())
                .spec(builder.build())
                .build();
    }
}
