package com.michelin.ns4kafka.cli;

import java.util.Optional;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.client.NonNamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "get" , description = "Get resources of a Namespace")
public class GetSubcommand extends AbstractJWTCommand implements Callable<Integer>{

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    NonNamespacedResourceClient nonNamespacedClient;

    @Option(names = {"-n", "--namespace"})
    String namespace;

    @Parameters(index = "0", description = "The name of the kind")
    String kind;

    @Parameters(index = "1", description = "The name of the resource")
    String name;

    @Override
    public Integer call() throws Exception {
        String token = getJWT();
        token = "Bearer " + token;
        Optional<ResourceDefinition> optionalResourceDefinition = manageResource.getResourceDefinitionFromCommandName(kind);
        ResourceDefinition resourceDefinition;
        try {
           resourceDefinition = optionalResourceDefinition.get();
        } catch(Exception e) {
            System.err.println("Can't find kind of resource named: " + name);
            return 2;
        }
        Resource resource;
        try {
            if(resourceDefinition.isNamespaced()) {
                resource = namespacedClient.get(namespace, resourceDefinition.getPath(), name, token);
            }
            else {
                System.err.println("Unimplemented for non namespaced resource");
                return 1;
            }
        } catch(HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch(status){
                default:
                System.err.println("Get command failed with message : " + e.getMessage());
            }
            return 1;
        }
        System.out.println(resource);
        return 0;
    }

}
