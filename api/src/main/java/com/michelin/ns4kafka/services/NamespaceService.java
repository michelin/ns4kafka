package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Singleton
public class NamespaceService {

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    public List<String> validate(Namespace namespace) {

        List<String> validationErrors = new ArrayList<>();
        if (namespaceRepository.findByName(namespace.getMetadata().getName()).isPresent()) {
            validationErrors.add("Namespace already exist");
        }

        if (kafkaAsyncExecutorConfigList.stream()
                .noneMatch(config -> config.getName().equals(namespace.getMetadata().getCluster()))) {
            validationErrors.add("Cluster doesn't exist");
        }
        if (namespaceRepository.findAllForCluster(namespace.getMetadata().getCluster()).stream()
                .anyMatch( namespace1 -> namespace1.getSpec().getKafkaUser().equals(namespace.getSpec().getKafkaUser()))) {
            validationErrors.add("KafkaUser already exist");
        }
        return validationErrors;
    }

    public Optional<Namespace> findByName(String namespace) {
        return namespaceRepository.findByName(namespace);
    }
}
