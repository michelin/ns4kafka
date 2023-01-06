package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ConnectCluster;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.ConnectClusterRepository;
import com.michelin.ns4kafka.utils.EncryptionUtils;
import com.nimbusds.jose.JOSEException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientException;
import io.micronaut.rxjava3.http.client.Rx3HttpClient;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class ConnectClusterService {
    @Inject
    AccessControlEntryService accessControlEntryService;

    @Inject
    ConnectClusterRepository connectClusterRepository;

    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfig;

    @Inject
    SecurityConfig securityConfig;

    @Inject
    @Client("/")
    Rx3HttpClient httpClient;

    /**
     * Find all self deployed Connect clusters
     * @return A list of Connect clusters
     */
    public List<ConnectCluster> findAll() {
        return connectClusterRepository.findAll();
    }

    /**
     * Find all self deployed Connect clusters for a given namespace with a given list of permissions
     * @param namespace The namespace
     * @param permissions The list of permission to filter on
     * @return A list of Connect clusters
     */
    public List<ConnectCluster> findAllByNamespace(Namespace namespace, List<AccessControlEntry.Permission> permissions) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace).stream()
                .filter(acl -> permissions.contains(acl.getSpec().getPermission()))
                .filter(acl -> acl.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT_CLUSTER).toList();

        return connectClusterRepository.findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(connector -> acls.stream().anyMatch(accessControlEntry ->
                        switch (accessControlEntry.getSpec().getResourcePatternType()) {
                    case PREFIXED ->
                            connector.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                    case LITERAL ->
                            connector.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                }))
                .collect(Collectors.toList());
    }

    /**
     * Find all self deployed Connect clusters whose namespace is owner
     * @param namespace The namespace
     * @return The list of owned Connect cluster
     */
    public List<ConnectCluster> findAllByNamespaceOwner(Namespace namespace) {
        return findAllByNamespace(namespace, List.of(AccessControlEntry.Permission.OWNER))
                .stream()
                .map(connectCluster -> ConnectCluster.builder()
                        .metadata(connectCluster.getMetadata())
                        .spec(ConnectCluster.ConnectClusterSpec.builder()
                                .url(connectCluster.getSpec().getUrl())
                                .username(connectCluster.getSpec().getUsername())
                                .password(EncryptionUtils.decryptAES256GCM(connectCluster.getSpec().getPassword(), securityConfig.getAes256EncryptionKey()))
                                .build())
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Find all self deployed Connect clusters whose namespace has write access
     * @param namespace The namespace
     * @return The list of Connect cluster with write access
     */
    public List<ConnectCluster> findAllByNamespaceWrite(Namespace namespace) {
        return findAllByNamespace(namespace, List.of(AccessControlEntry.Permission.OWNER, AccessControlEntry.Permission.WRITE));
    }

    /**
     * Find a self deployed Connect cluster by namespace and name with owner rights
     * @param namespace The namespace
     * @param connectClusterName The connect worker name
     * @return An optional connect worker
     */
    public Optional<ConnectCluster> findByNamespaceAndNameOwner(Namespace namespace, String connectClusterName) {
        return findAllByNamespaceOwner(namespace)
                .stream()
                .filter(connectCluster -> connectCluster.getMetadata().getName().equals(connectClusterName))
                .findFirst();
    }

    /**
     * Create a given connect worker
     * @param connectCluster The connect worker
     * @return The created connect worker
     */
    public ConnectCluster create(ConnectCluster connectCluster) throws IOException, JOSEException {
        if (StringUtils.hasText(connectCluster.getSpec().getPassword())) {
            connectCluster.getSpec()
                    .setPassword(EncryptionUtils.encryptAES256GCM(connectCluster.getSpec().getPassword(), securityConfig.getAes256EncryptionKey()));
        }
        return connectClusterRepository.create(connectCluster);
    }

    /**
     * Validate the given connect worker configuration for creation
     * @param connectCluster The connect worker to validate
     * @return A list of validation errors
     */
    public List<String> validateConnectClusterCreation(ConnectCluster connectCluster) {
        List<String> errors = new ArrayList<>();

        if (kafkaAsyncExecutorConfig.stream().anyMatch(cluster ->
                cluster.getConnects().entrySet().stream().anyMatch(entry -> entry.getKey().equals(connectCluster.getMetadata().getName())))) {
            errors.add(String.format("A Connect cluster is already defined globally with the name %s. Please provide a different name.", connectCluster.getMetadata().getName()));
        }

        try {
            MutableHttpRequest<?> request = HttpRequest.GET(new URL(connectCluster.getSpec().getUrl()) + "/connectors?expand=info&expand=status");
            if (StringUtils.hasText(connectCluster.getSpec().getUsername()) && StringUtils.hasText(connectCluster.getSpec().getPassword())){
                request.basicAuth(connectCluster.getSpec().getUsername(), connectCluster.getSpec().getPassword());
            }
            HttpResponse<?> response = httpClient.exchange(request).blockingFirst();
            if (!response.getStatus().equals(HttpStatus.OK)) {
                errors.add(String.format("The Connect cluster %s is not healthy (HTTP code %s).", connectCluster.getMetadata().getName(), response.getStatus().getCode()));
            }
        } catch (MalformedURLException e) {
            errors.add(String.format("The Connect cluster %s has a malformed URL \"%s\".", connectCluster.getMetadata().getName(), connectCluster.getSpec().getUrl()));
        } catch (HttpClientException e) {
            errors.add(String.format("The following error occurred trying to check the Connect cluster %s health: %s.", connectCluster.getMetadata().getName(), e.getMessage()));
        }

        return errors;
    }

    /**
     * Delete a given Connect cluster
     * @param connectCluster The Connect cluster
     */
    public void delete(ConnectCluster connectCluster) {
        connectClusterRepository.delete(connectCluster);
    }

    /**
     * Is given namespace owner of the given connect worker
     * @param namespace The namespace
     * @param connectCluster The connect cluster
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfConnectCluster(Namespace namespace, String connectCluster) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(),
                AccessControlEntry.ResourceType.CONNECT, connectCluster);
    }
}
