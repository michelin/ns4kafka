package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "list" , description = "List all resources of a Namespace")
public class ListSubcommand extends AbstractJWTCommand implements Callable<Integer>{

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    ClusterResourceClient nonNamespacedClient;

    @Option(names = {"-n", "--namespace"})
    String namespace = "";

    @Parameters(index = "0", description = "The name of the kind which you want the list")
    String name;

    @Value("${namespace.path}")
    private String namespaceConfig;

    @Override
    public Integer call() throws Exception {
        String token = getJWT();
        token = "Bearer " + token;
        String namespaceValue = namespaceConfig;
        if (!namespace.isEmpty()) {
            namespaceValue = namespace;
        }
        if (namespaceValue.isEmpty()){
            return 2;
        }
        Optional<ResourceDefinition> optionalResourceDefinition = manageResource.getResourceDefinitionFromCommandName(name);
        ResourceDefinition resourceDefinition;
        try {
           resourceDefinition = optionalResourceDefinition.get();
        } catch(Exception e) {
            System.out.println(Ansi.AUTO.string("@|bold,red Can't find resource named: |@") + name);
            return 2;
        }

        List<Resource> resources;
        try {
            if(resourceDefinition.isNamespaced()) {
                resources = namespacedClient.list(namespaceValue, resourceDefinition.getPath(), token);
            }
            else {
                resources = nonNamespacedClient.list(token,resourceDefinition.getPath());
            }
        } catch(HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch(status){
                default:
                System.out.println(Ansi.AUTO.string("@|bold,red List command failed with message : |@")+e.getMessage());
            }
            return 1;
        }
        if(resources.isEmpty()){
            System.out.println("No resources of type "+name+ " found.");
        }
        for (Resource resource : resources) {
            System.out.println(resource.getMetadata().getName());
        }
        return 0;
    }
}
