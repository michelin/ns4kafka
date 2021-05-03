package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "list" , description = "List all resources of a Namespace")
public class ListSubcommand implements Callable<Integer>{

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    ClusterResourceClient nonNamespacedClient;

    @Inject
    LoginService loginService;
    @Inject
    ApiResourcesService apiResourcesService;
    @Inject
    KafkactlConfig kafkactlConfig;

    @CommandLine.ParentCommand
    KafkactlCommand kafkactlCommand;
    @Parameters(index = "0", description = "The name of the kind which you want the list")
    String name;

    @Override
    public Integer call() throws Exception {

        boolean authenticated = loginService.doAuthenticate(kafkactlCommand.verbose);
        if (!authenticated) {
            return 1;
        }

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        Optional<ResourceDefinition> optionalResourceDefinition = apiResourcesService.getResourceDefinitionFromCommandName(name);
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
                resources = namespacedClient.list(namespace, resourceDefinition.getPath(), loginService.getAuthorization());
            }
            else {
                resources = nonNamespacedClient.list(loginService.getAuthorization(), resourceDefinition.getPath());
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
