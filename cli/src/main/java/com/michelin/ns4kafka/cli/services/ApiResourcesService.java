package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Singleton
public class ApiResourcesService {
    @Inject
    private ClusterResourceClient resourceClient;

    public List<ApiResource> getListResourceDefinition() {
        //TODO Add Cache to reduce the number of http requests
        return resourceClient.listResourceDefinitions();
    }

    public Optional<ApiResource> getResourceDefinitionFromKind(String kind) {
        List<ApiResource> apiResources = getListResourceDefinition();
        return apiResources.stream()
                .filter(resource -> resource.getKind().equals(kind))
                .findFirst();
    }
    public Optional<ApiResource> getResourceDefinitionFromCommandName(String name) {
        List<ApiResource> apiResources = getListResourceDefinition();
        return apiResources.stream()
                .filter(resource -> resource.getNames().contains(name))
                .findFirst();
    }
    public List<Resource> validateResourceTypes(List<Resource> resources) {
        List<String> allowedKinds = this.getListResourceDefinition()
                .stream()
                .map(ApiResource::getKind)
                .collect(Collectors.toList());
        return resources.stream()
                .filter(resource -> !allowedKinds.contains(resource.getKind()))
                .collect(Collectors.toList());

    }
}
