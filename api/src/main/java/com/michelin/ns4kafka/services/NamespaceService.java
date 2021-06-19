package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class NamespaceService {

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;
    @Inject
    TopicService topicService;
    @Inject
    RoleBindingService roleBindingService;
    @Inject
    AccessControlEntryService accessControlEntryService;
    @Inject
    KafkaConnectService kafkaConnectService;

    /**
     * Namespace validation in case of new namespace
     *
     * @param namespace
     * @return
     */
    public List<String> validateCreation(Namespace namespace) {

        List<String> validationErrors = new ArrayList<>();

        if (kafkaAsyncExecutorConfigList.stream()
                .noneMatch(config -> config.getName().equals(namespace.getMetadata().getCluster()))) {
            validationErrors.add("Invalid value " + namespace.getMetadata().getCluster()
                    + " for cluster: Cluster doesn't exist");
        }
        if (namespaceRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
                .anyMatch(namespace1 -> namespace1.getSpec().getKafkaUser().equals(namespace.getSpec().getKafkaUser()))) {
            validationErrors.add("Invalid value " + namespace.getSpec().getKafkaUser()
                    + " for user: KafkaUser already exists");
        }
        return validationErrors;
    }

    public List<String> validate(Namespace namespace) {
        return namespace.getSpec().getConnectClusters()
                .stream()
                .filter(connectCluster -> !connectClusterExists(namespace.getMetadata().getCluster(), connectCluster))
                .map(s -> "Invalid value " + s + " for Connect Cluster: Connect Cluster doesn't exist")
                .collect(Collectors.toList());
    }

    private boolean connectClusterExists(String kafkaCluster, String connectCluster) {
        return kafkaAsyncExecutorConfigList.stream()
                .anyMatch(kafkaAsyncExecutorConfig -> kafkaAsyncExecutorConfig.getName().equals(kafkaCluster) &&
                        kafkaAsyncExecutorConfig.getConnects().containsKey(connectCluster));
    }

    public Optional<Namespace> findByName(String namespace) {
        return namespaceRepository.findByName(namespace);
    }

    public Namespace createOrUpdate(Namespace namespace) {
        return namespaceRepository.createNamespace(namespace);
    }

    public void delete(Namespace namespace) {
        namespaceRepository.delete(namespace);
    }

    public List<Namespace> listAll() {
        return kafkaAsyncExecutorConfigList.stream()
                .map(KafkaAsyncExecutorConfig::getName)
                .flatMap(s -> namespaceRepository.findAllForCluster(s).stream())
                .collect(Collectors.toList());
    }

    public List<String> listAllNamespaceResources(Namespace namespace){
        //TODO rework xxxService implements NamespacedResourceService
        // Inject List<NamespacedResourceService> allServices
        // allServices.flatMap(x->x.findAllForNamespace(ns).stream())...
        return Stream.of(
                topicService.findAllForNamespace(namespace).stream()
                        .map(topic -> topic.getKind()+"/"+topic.getMetadata().getName()),
                kafkaConnectService.findAllForNamespace(namespace).stream()
                        .map(connector -> connector.getKind()+"/"+connector.getMetadata().getName()),
                accessControlEntryService.findAllForNamespace(namespace).stream()
                        .map(ace -> ace.getKind()+"/"+ace.getMetadata().getName()),
                roleBindingService.list(namespace.getMetadata().getName()).stream()
                        .map(roleBinding -> roleBinding.getKind()+"/"+roleBinding.getMetadata().getName())
                )
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
                .collect(Collectors.toList());
    }
}
