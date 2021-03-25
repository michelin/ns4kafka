package com.michelin.ns4kafka.services;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.michelin.ns4kafka.controllers.AdminController.NamespaceCreationRequest;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;

@Singleton
public class NamespaceService {

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigList;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    public List<String> validate(NamespaceCreationRequest namespaceCreationRequest) {

        List<String> validationErrors = new ArrayList<>();
        if (namespaceRepository.findByName(namespaceCreationRequest.getName()).isPresent()) {
            validationErrors.add("Namespace already exist");
        }

        if (kafkaAsyncExecutorConfigList.stream()
                .noneMatch(config -> config.getName().equals(namespaceCreationRequest.getCluster()))) {
            validationErrors.add("Cluster doesn't exist");
        }
        if (namespaceRepository.findAllForCluster(namespaceCreationRequest.getCluster()).stream().anyMatch(
                namespace -> namespace.getDefaulKafkatUser().equals(namespaceCreationRequest.getKafkaUser()))) {
            validationErrors.add("KafkaUser already exist");
        }
        return validationErrors;
    }
}
