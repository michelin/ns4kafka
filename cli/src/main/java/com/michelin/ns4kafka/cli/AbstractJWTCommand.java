package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;
import io.micronaut.core.io.IOUtils;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public abstract class AbstractJWTCommand {

    @Inject
    ManageResource manageResource;

    private final String path = System.getProperty("user.home")+"/.kafkactl/jwt";

    public String getJWT() {
        BufferedReader in;
        String jwt = null;
        try {
            in = new BufferedReader(new FileReader(path));
            jwt = IOUtils.readText(in);
        } catch (FileNotFoundException e) {
            System.out.println("Please login first.");
        } catch (IOException e) {
            System.out.println("Please login first.");
        }
        return jwt;
    }

    public static class ManageResource {

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

}
