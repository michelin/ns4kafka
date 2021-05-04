package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

        boolean authenticated = loginService.doAuthenticate(kafkactlCommand.verbose);
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

        List<String> contents;
        if (file.isPresent()) {
            // 1. list all files to process
            List<File> yamlFiles = listAllFiles(new File[]{file.get()}, recursive).collect(Collectors.toList());
            if (yamlFiles.isEmpty()) {
                System.out.println("Could not find files in " + file.get().getName());
                return 1;
            }
            // 2.a load each file in a String
            contents = readAllFiles(yamlFiles);
        } else {
            // 2.b load stin
            Scanner scanner = new Scanner(System.in);
            scanner.useDelimiter("\\Z");
            String stdin = scanner.next();
            contents = List.of(stdin);
            if (kafkactlCommand.verbose) {
                System.out.println("Loaded " + stdin.length() + " characters from STDIN");
            }
        }
        // 3. load all files into Resource Descriptors objects
        List<Resource> resources = loadAllResources(contents);
        if (kafkactlCommand.verbose) {
            System.out.println("Loaded Resources :");
            resources.forEach(resource -> System.out.println(resource.getKind() + "/" + resource.getMetadata().getName()));
            System.out.println("---");
        }

        // 4. process each document individually, return 0 when all succeed
        int errors = resources.stream().parallel().mapToInt(this::applyResource).sum();
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
                namespacedClient.apply(defaultNamespace, apiResource.getPath(), token, resource);
            } else {
                nonNamespacedClient.apply(token, apiResource.getPath(), resource);
            }
            System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS: |@") + apiResource.getKind() + "/" + resource.getMetadata().getName());

        } catch (HttpClientResponseException e) {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED |@") + apiResource.getKind() + "/" + resource.getMetadata().getName() + Ansi.AUTO.string("@|bold,red  failed with message : |@") + e.getMessage());
            return 1;
        }
        return 0;
    }

    private List<Resource> loadAllResources(List<String> fileContents) {
        return fileContents.stream()
                .flatMap(content -> StreamSupport.stream(
                        new Yaml(new Constructor(Resource.class)).loadAll(content).spliterator(), false)
                )
                .map(o -> (Resource) o)
                .collect(Collectors.toList());
    }

    private List<String> readAllFiles(List<File> files) {
        return files.stream()
                .map(File::toPath)
                .map(path -> {
                    try {
                        return Files.readString(path);
                    } catch (IOException e) {
                        System.out.println("Error reading file: " + e.getMessage());
                        return null;
                    }
                })
                .collect(Collectors.toList());
    }

    private Stream<File> listAllFiles(File[] rootDir, boolean recursive) {

        return Arrays.stream(rootDir).flatMap(currentElement -> {
            if (currentElement.isDirectory()) {
                return Stream.concat(
                        Stream.of(currentElement.listFiles(file -> file.isFile() && (file.getName().endsWith(".yaml") || file.getName().endsWith(".yml")))),
                        recursive ? listAllFiles(currentElement.listFiles(File::isDirectory), true) : Stream.empty()
                );
            } else {
                return Stream.of(currentElement);
            }
        });

    }
}
