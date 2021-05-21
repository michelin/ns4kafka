package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.YmlWriterService;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

@Command(name = "synchronize", description = "Synchronize topics for a namespace")
public class SynchronizeNamespaceSubcommand implements Callable<Integer> {

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    LoginService loginService;

    @CommandLine.ParentCommand
    KafkactlCommand kafkactlCommand;

    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    boolean dryRun;
    @Option(names = {"--yml"}, description = "Describe resource as yml format")
    boolean yml;

    public Integer call() throws IOException {

        // 1. Authent
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            return 1;
        }

        try {
            String defaultNamespace = kafkactlCommand.optionalNamespace.get();
            List<Resource> resources = namespacedClient.synchronize(defaultNamespace, loginService.getAuthorization(), dryRun);

            for (Resource resource : resources) {
                System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS synchronizing resource:|@") + resource.getKind() + "/" + resource.getMetadata().getName());
            }
            System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS synchronizing namespace:|@") + defaultNamespace);
            
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
