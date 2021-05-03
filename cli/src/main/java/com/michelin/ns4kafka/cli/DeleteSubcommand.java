package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
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
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "delete", description = "Delete a resource")
public class DeleteSubcommand implements Callable<Integer> {

    @Inject
    NamespacedResourceClient namespacedClient;
    @Inject
    ClusterResourceClient nonNamespacedClient;

    @Inject
    KafkactlConfig kafkactlConfig;

    @Inject
    LoginService loginService;
    @Inject
    ApiResourcesService apiResourcesService;

    @CommandLine.ParentCommand
    KafkactlCommand kafkactlCommand;
    @Parameters(index = "0", description = "The name of the kind")
    String kind;
    @Parameters(index = "1", description = "The name of the resource")
    String name;

    @Override
    public Integer call() {

        boolean authenticated = loginService.doAuthenticate(kafkactlCommand.verbose);
        if (!authenticated) {
            return 1;
        }

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        Optional<ResourceDefinition> optionalResourceDefinition = apiResourcesService.getResourceDefinitionFromCommandName(kind);
        ResourceDefinition resourceDefinition;
        try {
           resourceDefinition = optionalResourceDefinition.get();
        } catch(Exception e) {
            System.out.println(Ansi.AUTO.string("@|bold,red Can't find kind of resource named: |@") + name);
            return 2;
        }
        try {
            if(resourceDefinition.isNamespaced()) {
                namespacedClient.delete(namespace, resourceDefinition.getPath(), name, loginService.getAuthorization());
            }
            else {
                //nonNamespacedClient.delete(token);
            }
        } catch(HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch(status){
                default:
                System.err.println(Ansi.AUTO.string("@|bold,red Delete command failed with message : |@") + e.getMessage());
            }
            return 1;
        }

        System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS |@"));
        return 0;
    }

}
