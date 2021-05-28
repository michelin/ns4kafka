package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "synchronize", description = "Synchronize resources already present on the Kafka Cluster with ns4kafka")
public class SynchronizeSubcommand implements Callable<Integer> {

    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public NamespacedResourceClient namespacedClient;

    @Inject
    public LoginService loginService;

    @Inject
    public ApiResourcesService apiResourcesService;

    //TODO check native-image with ParentCommand again
    //@CommandLine.ParentCommand
    //KafkactlCommand kafkactlCommand;

    @Parameters(index = "0", description = "Resource type", arity = "1")
    public String resourceType;
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    public boolean dryRun;
    @Option(names = {"-n", "--namespace"}, description = "Override namespace defined in config or yaml resource", scope = CommandLine.ScopeType.INHERIT)
    public Optional<String> optionalNamespace;

    public Integer call() throws IOException {

        // 1. Authent
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            return 1;
        }
        
        String namespace = optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + resourceType + ": The server doesn't have resource type");
            return 1;
        }

        ApiResource apiResource = optionalApiResource.get();

        try {
            if (apiResource.isNamespaced()) {
                List<Resource> resources = namespacedClient.synchronize(namespace, apiResource.getPath(), loginService.getAuthorization(), dryRun);
                if (resources.size() == 0) {
                    System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS no resource to synchronize for namespace:|@") + namespace);
                    return 0;
                }
                for (Resource resource : resources) {
                    System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS synchronizing resource:|@") + resource.getKind() + "/" + resource.getMetadata().getName());
                }
            } else {
                System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + apiResource.getKind() + ": The server doesn't have implemented this");
                return 1;
            }
            System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS synchronizing namespace:|@") + namespace);
        } catch (HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch (status) {
                case BAD_REQUEST:
                    System.err.println(e.getMessage());
                    return 2;
                default:
                    System.err.println("Resource failed with message : " + e.getMessage());
            }
            return 1;
        } catch (NoSuchElementException e) {
            System.err.println(Ansi.AUTO.string("@|bold,red The namespace option is required for the synchronize command |@"));
            return 1;
        }
        return 0;

    }
}
