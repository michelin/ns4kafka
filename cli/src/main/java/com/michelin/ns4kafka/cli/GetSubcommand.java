package com.michelin.ns4kafka.cli;

import java.util.Optional;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;

import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "get" , description = "Get resources of a Namespace")
public class GetSubcommand extends AbstractResourceCommand implements Callable<Integer>{

    @Option(names = {"-n", "--namespace"})
    String namespace = "";

    @Parameters(index = "0", description = "The name of the kind")
    String kind;

    @Parameters(index = "1", description = "The name of the resource")
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

        Optional<ResourceDefinition> optionalResourceDefinition = manageResource.getResourceDefinitionFromCommandName(kind);
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
                resource = namespacedClient.get(namespaceValue, resourceDefinition.getPath(), name, token);
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