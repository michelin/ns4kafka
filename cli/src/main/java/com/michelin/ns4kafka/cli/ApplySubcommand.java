package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.FileService;
import com.michelin.ns4kafka.cli.services.LoginService;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.Callable;

@Command(name = "apply", description = "Create or update a resource")
public class ApplySubcommand implements Callable<Integer> {

    @Inject
    NamespacedResourceClient namespacedClient;
    @Inject
    ClusterResourceClient nonNamespacedClient;

    @Inject
    LoginService loginService;
    @Inject
    ApiResourcesService apiResourcesService;
    @Inject
    FileService fileService;
    @Inject
    KafkactlConfig kafkactlConfig;

    @CommandLine.ParentCommand
    KafkactlCommand kafkactlCommand;
    @Option(names = {"-f", "--file"}, description = "YAML File or Directory containing YAML resources")
    Optional<File> file;
    @Option(names = {"-R", "--recursive"}, description = "Enable recursive search of file")
    boolean recursive;
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    boolean dryRun;

    @Override
    public Integer call() throws Exception {

        if (dryRun) {
            System.out.println("Dry run execution");
        }

        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            // Autentication failed, stopping
            return 1;
        }

        // 0. Check STDIN and -f
        boolean hasStdin = System.in.available() > 0;
        // If we have none or both stdin and File set, we stop
        if (hasStdin == file.isPresent()) {
            System.out.println("Required one of -f or stdin");
            return 1;
        }

        List<Resource> resources;

        if (file.isPresent()) {
            // 1. list all files to process
            List<File> yamlFiles = fileService.computeYamlFileList(file.get(),recursive);
            if (yamlFiles.isEmpty()) {
                System.out.println("Could not find yaml/yml files in " + file.get().getName());
                return 1;
            }
            // 2 load each files
            resources = fileService.parseResourceListFromFiles(yamlFiles);
        } else {
            Scanner scanner = new Scanner(System.in);
            scanner.useDelimiter("\\Z");
            // 2 load STDIN
            resources = fileService.parseResourceListFromString(scanner.next());
        }

        // 3. process each document individually, return 0 when all succeed
        int errors = resources.stream().mapToInt(this::applyResource).sum();
        return errors > 0 ? 1 : 0;
    }

    private int applyResource(Resource resource) {
        String token = loginService.getAuthorization();

        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromKind(resource.getKind());
        if(optionalApiResource.isEmpty())
        {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + resource.getKind() + "/" + resource.getMetadata().getName()+": The server doesn't have resource type");
            return 1;
        }

        ApiResource apiResource = optionalApiResource.get();
        try {
            if (apiResource.isNamespaced()) {
                String yamlNamespace = resource.getMetadata().getNamespace();
                String defaultNamespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());
                if(yamlNamespace != null && defaultNamespace != null && !yamlNamespace.equals(defaultNamespace)){
                    System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + apiResource.getKind() + "/" + resource.getMetadata().getName()+": Namespace mismatch between kafkactl and yaml document");
                    return 1;
                }
                namespacedClient.apply(defaultNamespace, apiResource.getPath(), token, resource, dryRun);
            } else {
                nonNamespacedClient.apply(token, apiResource.getPath(), resource, dryRun);
            }
            System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS: |@") + apiResource.getKind() + "/" + resource.getMetadata().getName());

        } catch (HttpClientResponseException e) {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED |@") + apiResource.getKind() + "/" + resource.getMetadata().getName() + Ansi.AUTO.string("@|bold,red  failed with message : |@") + e.getMessage());
            return 1;
        }
        return 0;
    }
}
