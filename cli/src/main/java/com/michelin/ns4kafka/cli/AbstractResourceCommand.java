package com.michelin.ns4kafka.cli;

import java.util.List;
import java.util.Optional;

import javax.inject.Inject;

import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.client.NonNamespacedResourceClient;
import com.michelin.ns4kafka.cli.client.ResourceDefinitionClient;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;

public abstract class AbstractResourceCommand extends AbstractJWTCommand {

    @Inject
    ManageResource manageResource;

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    NonNamespacedResourceClient nonNamespacedClient;

    public static class ManageResource {

        @Inject
        private ResourceDefinitionClient resourceClient;

        public List<ResourceDefinition> getListResourceDefinition() {
            //TODO Add Cache to reduce the number of http requests
            return resourceClient.getResource();
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
}
