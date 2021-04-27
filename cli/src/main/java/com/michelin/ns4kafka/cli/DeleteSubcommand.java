package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "delete", description = "Delete a resource")
public class DeleteSubcommand extends AbstractJWTCommand implements Callable<Integer> {

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    ClusterResourceClient nonNamespacedClient;

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
        try {
            if(resourceDefinition.isNamespaced()) {
                namespacedClient.delete(namespaceValue, resourceDefinition.getPath(), name, token);
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
