package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
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

    /**
     * Namespace validation in case of new namespace
     *
     * @param namespace
     * @return
     */
    public List<String> validateCreation(Namespace namespace) {

        List<String> validationErrors = new ArrayList<>();

        if(namespace.getMetadata().getName().equals(Namespace.ADMIN_NAMESPACE)){
            validationErrors.add("Invalid value " + Namespace.ADMIN_NAMESPACE
                    + " for namespace: Reserved name");
        }

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

    public Optional<Namespace> findByName(String namespace) {
        return namespaceRepository.findByName(namespace);
    }

    public Namespace createOrUpdate(Namespace namespace){
        return namespaceRepository.createNamespace(namespace);
    }
}
