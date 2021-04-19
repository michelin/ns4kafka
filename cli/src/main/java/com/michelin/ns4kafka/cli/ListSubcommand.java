package com.michelin.ns4kafka.cli;

import java.util.List;
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

@Command(name = "list" , description = "List all resources of a Namespace")
public class ListSubcommand extends AbstractJWTCommand implements Callable<Integer>{

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    NonNamespacedResourceClient nonNamespacedClient;

    @Option(names = {"-n", "--namespace"})
    String namespace = null;

    @Parameters(index = "0", description = "The name of the kind which you want the list")
    String name;

    @Override
    public Integer call() throws Exception {
        String token = getJWT();
        token = "Bearer " + token;
        //TODO add Config namespace
        String namespaceValue = namespace;
        if (namespaceValue.isEmpty()){
            return 2;
        }
        Optional<ResourceDefinition> optionalResourceDefinition = manageResource.getResourceDefinitionFromCommandName(name);
        ResourceDefinition resourceDefinition;
        try {
           resourceDefinition = optionalResourceDefinition.get();
        } catch(Exception e) {
            System.err.println("Can't find resource named: " + name);
            return 2;
        }

        List<Resource> resources;
        try {
            if(resourceDefinition.isNamespaced()) {
                resources = namespacedClient.list(namespace, resourceDefinition.getPath(), token);
            }
            else {
                resources = nonNamespacedClient.list(token);
            }
        } catch(HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch(status){
                default:
                System.err.println("List command failed with message : "+e.getMessage());
            }
            return 1;
        }
        for (Resource resource : resources) {
            System.out.println(resource.getMetadata().getName());
        }
        return 0;
    }
}
