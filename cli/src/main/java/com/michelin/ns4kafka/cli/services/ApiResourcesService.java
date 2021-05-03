package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Singleton
public class ApiResourcesService {
    @Inject
    private ClusterResourceClient resourceClient;

    public List<ResourceDefinition> getListResourceDefinition() {
        //TODO Add Cache to reduce the number of http requests
        return resourceClient.listResourceDefinitions();
    }

    public Optional<ResourceDefinition> getResourceDefinitionFromKind(String kind) {
        List<ResourceDefinition> resourceDefinitions = getListResourceDefinition();
        return resourceDefinitions.stream()
                .filter(resource -> resource.getKind().equals(kind))
                .findFirst();
    }
    public Optional<ResourceDefinition> getResourceDefinitionFromCommandName(String name) {
        List<ResourceDefinition> resourceDefinitions = getListResourceDefinition();
        return resourceDefinitions.stream()
                .filter(resource -> resource.getNames().contains(name))
                .findFirst();
    }
}
