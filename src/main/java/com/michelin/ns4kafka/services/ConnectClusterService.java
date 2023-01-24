package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.config.SecurityConfig;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ConnectCluster;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.ConnectClusterRepository;
import com.michelin.ns4kafka.utils.EncryptionUtils;
import com.nimbusds.jose.JOSEException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
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

    /**
     * The default format string for aes 256 convertion.
     */
    private static final String DEFAULT_FORMAT = "${aes256:%s}";

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
    RxHttpClient httpClient;

    /**
     * Find all self deployed Connect clusters
     *
     * @return A list of Connect clusters
     */
    public List<ConnectCluster> findAll() {
        return connectClusterRepository.findAll();
    }

    /**
     * Find all self deployed Connect clusters for a given namespace with a given list of permissions
     *
     * @param namespace   The namespace
     * @param permissions The list of permission to filter on
     * @return A list of Connect clusters
     */
    public List<ConnectCluster> findAllByNamespace(Namespace namespace, List<AccessControlEntry.Permission> permissions) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace).stream()
                .filter(acl -> permissions.contains(acl.getSpec().getPermission()))
                .filter(acl -> acl.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT_CLUSTER)
                .collect(Collectors.toList());

        return connectClusterRepository.findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(connector -> acls.stream().anyMatch(accessControlEntry -> {
                    switch (accessControlEntry.getSpec().getResourcePatternType()) {
                        case PREFIXED:
                            return connector.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                        case LITERAL:
                            return connector.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                    }

                    return false;
                }))
                .collect(Collectors.toList());
    }

    /**
     * Find all self deployed Connect clusters whose namespace is owner
     *
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
                                .aes256Key(EncryptionUtils.decryptAES256GCM(connectCluster.getSpec().getAes256Key(), securityConfig.getAes256EncryptionKey()))
                                .aes256Salt(EncryptionUtils.decryptAES256GCM(connectCluster.getSpec().getAes256Salt(), securityConfig.getAes256EncryptionKey()))
                                .aes256Format(connectCluster.getSpec().getAes256Format())
                                .build())
                        .build())
                .collect(Collectors.toList());
    }

    /**
     * Find all self deployed Connect clusters whose namespace has write access
     *
     * @param namespace The namespace
     * @return The list of Connect cluster with write access
     */
    public List<ConnectCluster> findAllByNamespaceWrite(Namespace namespace) {
        return findAllByNamespace(namespace, List.of(AccessControlEntry.Permission.OWNER, AccessControlEntry.Permission.WRITE));
    }

    /**
     * Find a self deployed Connect cluster by namespace and name with owner rights
     *
     * @param namespace          The namespace
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
     *
     * @param connectCluster The connect worker
     * @return The created connect worker
     */
    public ConnectCluster create(ConnectCluster connectCluster) throws IOException, JOSEException {
        // encrypt password if present
        if (StringUtils.isNotBlank(connectCluster.getSpec().getPassword())) {
            connectCluster.getSpec()
                    .setPassword(EncryptionUtils.encryptAES256GCM(connectCluster.getSpec().getPassword(), securityConfig.getAes256EncryptionKey()));
        }

        // encrypt aes256 key if present
        if (StringUtils.isNotBlank(connectCluster.getSpec().getAes256Key())) {
            connectCluster.getSpec()
                    .setAes256Key(EncryptionUtils.encryptAES256GCM(connectCluster.getSpec().getAes256Key(), securityConfig.getAes256EncryptionKey()));
        }

        // encrypt aes256 salt if present
        if (StringUtils.isNotBlank(connectCluster.getSpec().getAes256Salt())) {
            connectCluster.getSpec()
                    .setAes256Salt(EncryptionUtils.encryptAES256GCM(connectCluster.getSpec().getAes256Salt(), securityConfig.getAes256EncryptionKey()));
        }

        return connectClusterRepository.create(connectCluster);
    }

    /**
     * Validate the given connect worker configuration for creation
     *
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
            if (StringUtils.isNotBlank(connectCluster.getSpec().getUsername()) && StringUtils.isNotBlank(connectCluster.getSpec().getPassword())) {
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

        // If properties "aes256Key" or aes256Salt is present, both properties are required.
        if (StringUtils.isNotBlank(connectCluster.getSpec().getAes256Key()) ^ StringUtils.isNotBlank(connectCluster.getSpec().getAes256Salt())) {
            errors.add(String.format("The Connect cluster \"%s\" \"aes256Key\" and \"aes256Salt\" spec are required to activate the encryption.", connectCluster.getMetadata().getName()));
        }

        return errors;
    }

    /**
     * Validate the given connect worker has configuration for vaults
     *
     * @param connectCluster The Kafka connect worker to validate
     * @return A list of validation errors
     */
    public List<String> validateConnectClusterVault(final Namespace namespace, final String connectCluster) {
        final var errors = new ArrayList<String>();

        var kafkaConnect = this.findAllByNamespaceWrite(namespace)
                .stream()
                .filter(cc -> cc.getMetadata().getName().equals(connectCluster))
                .findFirst();

        if (kafkaConnect.isEmpty()) {
            errors.add(String.format("No Connect cluster exists with the name %s. Please provide a different name.", connectCluster));
            return errors;
        }

        try {
            MutableHttpRequest<?> request = HttpRequest.GET(new URL(kafkaConnect.get().getSpec().getUrl()) + "/connectors?expand=info&expand=status");
            if (StringUtils.isNotBlank(kafkaConnect.get().getSpec().getUsername()) && StringUtils.isNotBlank(kafkaConnect.get().getSpec().getPassword())) {
                request.basicAuth(kafkaConnect.get().getSpec().getUsername(), kafkaConnect.get().getSpec().getPassword());
            }
            HttpResponse<?> response = httpClient.exchange(request).blockingFirst();
            if (!response.getStatus().equals(HttpStatus.OK)) {
                errors.add(String.format("The Connect cluster %s is not healthy (HTTP code %s).", connectCluster, response.getStatus().getCode()));
            }
        } catch (MalformedURLException e) {
            errors.add(String.format("The Connect cluster %s has a malformed URL \"%s\".", connectCluster, connectCluster));
        } catch (HttpClientException e) {
            errors.add(String.format("The following error occurred trying to check the Connect cluster %s health: %s.", connectCluster, e.getMessage()));
        }

        // If properties "aes256Key" or aes256Salt is present, both properties are required.
        if (StringUtils.isBlank(kafkaConnect.get().getSpec().getAes256Key())) {
            errors.add(String.format("The Connect cluster \"%s\" does not contain any aes 256 key in its configuration.", connectCluster));
        }

        if (StringUtils.isBlank(kafkaConnect.get().getSpec().getAes256Salt())) {
            errors.add(String.format("The Connect cluster \"%s\" does not contain any aes 256 salt in its configuration.", connectCluster));
        }

        return errors;
    }

    /**
     * Delete a given Connect cluster
     *
     * @param connectCluster The Connect cluster
     */
    public void delete(ConnectCluster connectCluster) {
        connectClusterRepository.delete(connectCluster);
    }

    /**
     * Is given namespace owner of the given connect worker
     *
     * @param namespace      The namespace
     * @param connectCluster The Kafka connect cluster
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfConnectCluster(Namespace namespace, String connectCluster) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(),
                AccessControlEntry.ResourceType.CONNECT, connectCluster);
    }

    /**
     * Is given namespace allowed (Owner or Writer) for the given connect worker
     *
     * @param namespace      The namespace
     * @param connectCluster The Kafka connect cluster
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceAllowedForConnectCluster(Namespace namespace, String connectCluster) {
        return this.findAllByNamespaceWrite(namespace)
                .stream()
                .anyMatch(kafkaConnect -> kafkaConnect.getMetadata().getName().equals(connectCluster));
    }

    /**
     * Vault a password for a specific namespace and a kafka connect cluster.
     *
     * @param namespace      The namespace that need an encrypted password.
     * @param connectCluster The kafka connect cluster for which to encrypt the password.
     * @param password       The password to encrypt.
     * @return The encrypted password.
     */
    public String vaultPassword(final Namespace namespace, final String connectCluster, final String password) {
        var kafkaConnect = this.findAllByNamespaceWrite(namespace)
                .stream()
                .filter(cc -> cc.getMetadata().getName().equals(connectCluster))
                .findFirst();
        if (kafkaConnect.isEmpty()) {
            return "";
        }

        final String aes256Key = EncryptionUtils.decryptAES256GCM(kafkaConnect.get().getSpec().getAes256Key(), securityConfig.getAes256EncryptionKey());
        final String aes256Salt = EncryptionUtils.decryptAES256GCM(kafkaConnect.get().getSpec().getAes256Salt(), securityConfig.getAes256EncryptionKey());
        final String aes256Format = kafkaConnect.get().getSpec().getAes256Format() != null && !kafkaConnect.get().getSpec().getAes256Format().isEmpty() ?
                kafkaConnect.get().getSpec().getAes256Format() : DEFAULT_FORMAT;
        return String.format(aes256Format, EncryptionUtils.encryptAES256(password, aes256Key, aes256Salt));
    }
}
