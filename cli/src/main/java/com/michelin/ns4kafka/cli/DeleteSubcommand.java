package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "delete", description = "Delete a resource")
public class DeleteSubcommand implements Callable<Integer> {

    @Inject
    public NamespacedResourceClient namespacedClient;
    @Inject
    public ClusterResourceClient nonNamespacedClient;

    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public LoginService loginService;
    @Inject
    public ApiResourcesService apiResourcesService;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;
    @Parameters(index = "0", description = "Resource type", arity = "1")
    public String resourceType;
    @Parameters(index = "1", description = "Resource name", arity = "1")
    public String name;
    @Option(names = {"--dry-run"}, description = "Does not persist operation. Validate only")
    public boolean dryRun;
    @Option(names = {"-n", "--namespace"}, description = "Override namespace defined in config or yaml resource", scope = CommandLine.ScopeType.INHERIT)
    public Optional<String> optionalNamespace;

    @Override
    public Integer call() {

        if (dryRun) {
            System.out.println("Dry run execution");
        }

        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            return 1;
        }

        String namespace = optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + resourceType + "/" + name + ": The server doesn't have resource type");
            return 1;
        }

        ApiResource apiResource = optionalApiResource.get();
        try {
            if (apiResource.isNamespaced()) {
                namespacedClient.delete(namespace, apiResource.getPath(), name, loginService.getAuthorization(), dryRun);
            } else {
                System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + apiResource.getKind() + "/" + name + ": The server doesn't have implemented this");
                return 1;
            }
            System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS |@"));
        } catch (HttpClientResponseException e) {
            System.err.println(Ansi.AUTO.string("@|bold,red Delete command failed with message : |@") + e.getMessage());
            return 1;
        }
        return 0;
    }

}
