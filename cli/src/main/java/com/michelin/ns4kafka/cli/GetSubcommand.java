package com.michelin.ns4kafka.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "get" , description = "Get resources of a Namespace")
public class GetSubcommand implements Callable<Integer>{

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
    @Parameters(index = "0", description = "The name of the kind")
    String kind;
    @Parameters(index = "1", description = "The name of the resource")
    String name;

    @Override
    public Integer call() throws Exception {

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
        Resource resource;
        try {
            if(resourceDefinition.isNamespaced()) {
                resource = namespacedClient.get(namespace, resourceDefinition.getPath(), name, loginService.getAuthorization());
            }
            else {
                System.err.println(Ansi.AUTO.string("@|bold,red Unimplemented for non namespaced resource |@"));
                return 1;
            }
        } catch(HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch(status){
                default:
                System.err.println(Ansi.AUTO.string("@|bold,red Get command failed with message : |@") + e.getMessage());
            }
            return 1;
        }
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(resource));
        return 0;
    }

}
