package com.michelin.ns4kafka.cli.services;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class ResourceService {

    @Inject
    NamespacedResourceClient namespacedClient;
    @Inject
    ClusterResourceClient nonNamespacedClient;

    @Inject
    LoginService loginService;

    public List<Resource> listAll(List<ApiResource> apiResources, String namespace) {
        return apiResources
                .stream()
                .flatMap(apiResource -> listResourcesWithType(apiResource, namespace).stream())
                .collect(Collectors.toList());
    }

    public List<Resource> listResourcesWithType(ApiResource apiResource, String namespace) {
        List<Resource> resources;
        if (apiResource.isNamespaced()) {
            try {
                resources = namespacedClient.list(namespace, apiResource.getPath(), loginService.getAuthorization());
            } catch (HttpClientResponseException e) {
                System.out.println("Error during list for resource type " + apiResource.getKind() + ": " + e.getMessage());
                resources = List.of();
            }
        } else {
            resources = nonNamespacedClient.list(loginService.getAuthorization(), apiResource.getPath());
        }
        return resources;
    }

    public Resource getSingleResourceWithType(ApiResource apiResource, String namespace, String resourceName) {
        try {
            if (apiResource.isNamespaced()) {
                return namespacedClient.get(namespace, apiResource.getPath(), resourceName, loginService.getAuthorization());
            } else {
                return nonNamespacedClient.get(loginService.getAuthorization(), apiResource.getKind(), resourceName);
            }
        } catch (Exception e) {
            System.out.println("Error during get for resource type " + apiResource.getKind() + "/" + resourceName + ": " + e.getMessage());
        }

        return null;
    }

    public Resource apply(ApiResource apiResource, String namespace, Resource resource, boolean dryRun) {
        try {
            Resource merged;
            if (apiResource.isNamespaced()) {
                return namespacedClient.apply(namespace, apiResource.getPath(), loginService.getAuthorization(), resource, dryRun);
            } else {
                return nonNamespacedClient.apply(loginService.getAuthorization(), apiResource.getPath(), resource, dryRun);
            }
        } catch (HttpClientResponseException e) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,red FAILED |@") + apiResource.getKind() + "/" + resource.getMetadata().getName() + CommandLine.Help.Ansi.AUTO.string("@|bold,red failed with message : |@") + e.getMessage());
        }
        return null;
    }
    public boolean delete(ApiResource apiResource, String namespace, String resource, boolean dryRun) {
        try {
            if (apiResource.isNamespaced()) {
                namespacedClient.delete(namespace, apiResource.getPath(), resource, loginService.getAuthorization(), dryRun);
                return true;
            } else {
                System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,red FAILED: |@") + apiResource.getKind() + "/" + resource + ": The server doesn't have implemented this");
            }
        } catch (HttpClientResponseException e) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,red FAILED |@") + apiResource.getKind() + "/" + resource + CommandLine.Help.Ansi.AUTO.string("@|bold,red failed with message : |@") + e.getMessage());
        }
        return false;
    }
    public List<Resource> synchronizeAll(List<ApiResource> apiResources, String namespace, boolean dryRun) {
        return apiResources
                .stream()
                .flatMap(apiResource -> synchronizeResourcesWithType(apiResource, namespace, dryRun).stream())
                .collect(Collectors.toList());
    }

    private List<Resource> synchronizeResourcesWithType(ApiResource apiResource, String namespace, boolean dryRun) {
        List<Resource> resources;

        try {
            resources = namespacedClient.synchronize(namespace, apiResource.getPath(), loginService.getAuthorization(), dryRun);
        } catch (HttpClientResponseException e) {
            System.out.println("Error during synchronize for resource type " + apiResource.getKind() + ": " + e.getMessage());
            resources = List.of();
        }

        return resources;
    }
}
