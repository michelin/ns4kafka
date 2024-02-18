package com.michelin.ns4kafka.services;

import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidNamespaceNoCluster;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidNamespaceUserAlreadyExist;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.utils.exceptions.error.ValidationError;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Service to manage the namespaces.
 */
@Singleton
public class NamespaceService {
    @Inject
    NamespaceRepository namespaceRepository;

    @Inject
    List<ManagedClusterProperties> managedClusterPropertiesList;

    @Inject
    TopicService topicService;

    @Inject
    RoleBindingService roleBindingService;

    @Inject
    AccessControlEntryService accessControlEntryService;

    @Inject
    ConnectorService connectorService;

    @Inject
    ConnectClusterService connectClusterService;

    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * Validate new namespace creation.
     *
     * @param namespace The namespace to create
     * @return A list of validation errors
     */
    public List<String> validateCreation(Namespace namespace) {
        List<String> validationErrors = new ArrayList<>();

        if (managedClusterPropertiesList.stream()
            .noneMatch(config -> config.getName().equals(namespace.getMetadata().getCluster()))) {
            validationErrors.add(invalidNamespaceNoCluster(namespace.getMetadata().getCluster()));
        }

        if (namespaceRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
            .anyMatch(namespace1 -> namespace1.getSpec().getKafkaUser().equals(namespace.getSpec().getKafkaUser()))) {
            validationErrors.add(invalidNamespaceUserAlreadyExist(namespace.getSpec().getKafkaUser()));
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
        return namespace.getSpec().getConnectClusters()
            .stream()
            .filter(connectCluster -> !connectClusterExists(namespace.getMetadata().getCluster(), connectCluster))
            .map(ValidationError::invalidNamespaceNoConnectCluster)
            .toList();
    }

    /**
     * Check if a given Connect cluster exists on a given Kafka cluster.
     *
     * @param kafkaCluster   The Kafka cluster
     * @param connectCluster The Connect cluster
     * @return true it does, false otherwise
     */
    private boolean connectClusterExists(String kafkaCluster, String connectCluster) {
        return managedClusterPropertiesList.stream()
            .anyMatch(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster)
                && kafkaAsyncExecutorConfig.getConnects().containsKey(connectCluster));
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
     * List all namespaces.
     *
     * @return The list of namespaces
     */
    public List<Namespace> listAll() {
        return managedClusterPropertiesList.stream()
            .map(ManagedClusterProperties::getName)
            .flatMap(s -> namespaceRepository.findAllForCluster(s).stream())
            .toList();
    }

    /**
     * List all resources of a namespace.
     *
     * @param namespace The namespace
     * @return The list of resources
     */
    public List<String> listAllNamespaceResources(Namespace namespace) {
        return Stream.of(
                topicService.findAllForNamespace(namespace).stream()
                    .map(topic -> topic.getKind() + "/" + topic.getMetadata().getName()),
                connectorService.findAllForNamespace(namespace).stream()
                    .map(connector -> connector.getKind() + "/" + connector.getMetadata().getName()),
                connectClusterService.findAllByNamespaceOwner(namespace).stream()
                    .map(connectCluster -> connectCluster.getKind() + "/" + connectCluster.getMetadata().getName()),
                accessControlEntryService.findAllForNamespace(namespace).stream()
                    .map(ace -> ace.getKind() + "/" + ace.getMetadata().getName()),
                resourceQuotaService.findByNamespace(namespace.getMetadata().getName()).stream()
                    .map(resourceQuota -> resourceQuota.getKind() + "/" + resourceQuota.getMetadata().getName()),
                roleBindingService.list(namespace.getMetadata().getName()).stream()
                    .map(roleBinding -> roleBinding.getKind() + "/" + roleBinding.getMetadata().getName())
            )
            .reduce(Stream::concat)
            .orElseGet(Stream::empty)
            .toList();
    }
}
