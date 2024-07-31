package com.michelin.ns4kafka.service;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNamespaceNoCluster;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidNamespaceUserAlreadyExist;
import static com.michelin.ns4kafka.util.enumation.Kind.ACCESS_CONTROL_ENTRY;
import static com.michelin.ns4kafka.util.enumation.Kind.CONNECTOR;
import static com.michelin.ns4kafka.util.enumation.Kind.CONNECT_CLUSTER;
import static com.michelin.ns4kafka.util.enumation.Kind.RESOURCE_QUOTA;
import static com.michelin.ns4kafka.util.enumation.Kind.ROLE_BINDING;
import static com.michelin.ns4kafka.util.enumation.Kind.TOPIC;

import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.util.FormatErrorUtils;
import com.michelin.ns4kafka.util.RegexUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.kafka.common.config.TopicConfig;

/**
 * Service to manage the namespaces.
 */
@Singleton
public class NamespaceService {
    private static final List<String> NOT_EDITABLE_CONFIGS = List.of(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG);

    @Inject
    NamespaceRepository namespaceRepository;

    @Inject
    List<ManagedClusterProperties> managedClusterProperties;

    @Inject
    TopicService topicService;

    @Inject
    RoleBindingService roleBindingService;

    @Inject
    AclService aclService;

    @Inject
    ConnectorService connectorService;

    @Inject
    ConnectClusterService connectClusterService;

    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * List all namespaces.
     *
     * @return The list of namespaces
     */
    public List<Namespace> findAll() {
        return managedClusterProperties
            .stream()
            .map(ManagedClusterProperties::getName)
            .flatMap(s -> namespaceRepository.findAllForCluster(s).stream())
            .toList();
    }

    /**
     * List all namespaces, filtered by name parameter.
     *
     * @param name The name filter
     * @return The list of namespaces
     */
    public List<Namespace> findByWildcardName(String name) {
        List<String> nameFilterPatterns = RegexUtils.wildcardStringsToRegexPatterns(List.of(name));
        return findAll()
            .stream()
            .filter(ns -> RegexUtils.filterByPattern(ns.getMetadata().getName(), nameFilterPatterns))
            .toList();
    }

    /**
     * Find a namespace by name.
     *
     * @param namespace The namespace
     * @return An optional namespace
     */
    public Optional<Namespace> findByName(String namespace) {
        return namespaceRepository.findByName(namespace);
    }

    /**
     * Validate new namespace creation.
     *
     * @param namespace The namespace to create
     * @return A list of validation errors
     */
    public List<String> validateCreation(Namespace namespace) {
        List<String> validationErrors = new ArrayList<>();

        if (managedClusterProperties
            .stream()
            .noneMatch(config -> config.getName().equals(namespace.getMetadata().getCluster()))) {
            validationErrors.add(invalidNamespaceNoCluster(namespace.getMetadata().getCluster()));
        }

        return validationErrors;
    }

    /**
     * Validate the Connect clusters of the namespace.
     *
     * @param namespace The namespace
     * @return A list of validation errors
     */
    public List<String> validate(Namespace namespace) {
        List<String> validationErrors = new ArrayList<>();

        Optional<ManagedClusterProperties> managedCluster = managedClusterProperties
            .stream()
            .filter(cluster -> namespace.getMetadata().getCluster().equals(cluster.getName()))
            .findFirst();

        if (managedCluster.isPresent()) {
            if (managedCluster.get().isConfluentCloud()) {
                validationErrors.addAll(NOT_EDITABLE_CONFIGS
                    .stream()
                    .filter(config -> namespace.getSpec().getTopicValidator().getValidationConstraints()
                        .containsKey(config))
                    .map(FormatErrorUtils::invalidNamespaceTopicValidatorKeyConfluentCloud)
                    .toList());
            }

            validationErrors.addAll(namespace.getSpec().getConnectClusters()
                .stream()
                .filter(connectCluster -> !managedCluster.get().getConnects().containsKey(connectCluster))
                .map(FormatErrorUtils::invalidNamespaceNoConnectCluster)
                .toList());
        }

        if (namespaceRepository.findAllForCluster(namespace.getMetadata().getCluster())
            .stream()
            .filter(foundNamespace -> !foundNamespace.getMetadata().getName().equals(namespace.getMetadata().getName()))
            .anyMatch(foundNamespace -> foundNamespace.getSpec().getKafkaUser()
                .equals(namespace.getSpec().getKafkaUser()))) {
            validationErrors.add(invalidNamespaceUserAlreadyExist(namespace.getSpec().getKafkaUser()));
        }

        return validationErrors;
    }

    /**
     * Create or update a namespace.
     *
     * @param namespace The namespace to create or update
     * @return The created or updated namespace
     */
    public Namespace createOrUpdate(Namespace namespace) {
        return namespaceRepository.createNamespace(namespace);
    }

    /**
     * Delete a namespace.
     *
     * @param namespace The namespace to delete
     */
    public void delete(Namespace namespace) {
        namespaceRepository.delete(namespace);
    }

    /**
     * List all resources of a namespace.
     *
     * @param namespace The namespace
     * @return The list of resources
     */
    public List<String> findAllResourcesByNamespace(Namespace namespace) {
        return Stream.of(
                topicService.findAllForNamespace(namespace).stream()
                    .map(topic -> TOPIC + "/" + topic.getMetadata().getName()),
                connectorService.findAllForNamespace(namespace).stream()
                    .map(connector -> CONNECTOR + "/" + connector.getMetadata().getName()),
                connectClusterService.findAllByNamespaceWithOwnerPermission(namespace).stream()
                    .map(connectCluster -> CONNECT_CLUSTER + "/" + connectCluster.getMetadata().getName()),
                aclService.findAllForNamespace(namespace).stream()
                    .map(ace -> ACCESS_CONTROL_ENTRY + "/" + ace.getMetadata().getName()),
                resourceQuotaService.findByNamespace(namespace.getMetadata().getName()).stream()
                    .map(resourceQuota -> RESOURCE_QUOTA + "/" + resourceQuota.getMetadata().getName()),
                roleBindingService.findAllForNamespace(namespace.getMetadata().getName()).stream()
                    .map(roleBinding -> ROLE_BINDING + "/" + roleBinding.getMetadata().getName())
            )
            .reduce(Stream::concat)
            .orElseGet(Stream::empty)
            .toList();
    }
}
