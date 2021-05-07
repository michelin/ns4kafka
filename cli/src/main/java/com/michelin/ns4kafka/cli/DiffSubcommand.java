
package com.michelin.ns4kafka.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.FileService;
import com.michelin.ns4kafka.cli.services.LoginService;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

@Command(name = "diff", description = "Get differences between the new resources and the old resource")
public class DiffSubcommand implements Callable<Integer> {

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

    @Override
    public Integer call() throws Exception {

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
        int errors = resources.stream().mapToInt(this::diffResource).sum();
        return errors > 0 ? 1 : 0;
    }

    private int diffResource(Resource resource) {
        String token = loginService.getAuthorization();

        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromKind(resource.getKind());
        if(optionalApiResource.isEmpty())
        {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + resource.getKind() + "/" + resource.getMetadata().getName()+": The server doesn't have resource type");
            return 1;
        }

        ApiResource apiResource = optionalApiResource.get();
        // 1. Get the current resource
        Resource oldResource;
        try {
            if (apiResource.isNamespaced()) {
                String yamlNamespace = resource.getMetadata().getNamespace();
                String defaultNamespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());
                if (yamlNamespace != null && defaultNamespace != null && !yamlNamespace.equals(defaultNamespace)) {
                    System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + apiResource.getKind() + "/" + resource.getMetadata().getName()+": Namespace mismatch between kafkactl and yaml document");
                    return 1;
                }
                oldResource = namespacedClient.get(defaultNamespace, apiResource.getPath(), resource.getMetadata().getName(), token);
            } else {
                System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + apiResource.getKind() + "/" + resource.getMetadata().getName()+": Unimplemented for non namespaced resource");
                return 1;
            }

        } catch (HttpClientResponseException e) {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED |@") + apiResource.getKind() + "/" + resource.getMetadata().getName() + Ansi.AUTO.string("@|bold,red  failed with message : |@") + e.getMessage());
            return 1;
        }

        // 2. Get the new resource
        Resource newResource;
        try {
            String yamlNamespace = resource.getMetadata().getNamespace();
            String defaultNamespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());
            if (yamlNamespace != null && defaultNamespace != null && !yamlNamespace.equals(defaultNamespace)) {
                System.out.println(Ansi.AUTO.string("@|bold,red FAILED: |@") + apiResource.getKind() + "/" + resource.getMetadata().getName()+": Namespace mismatch between kafkactl and yaml document");
                return 1;
            }
            newResource = namespacedClient.apply(defaultNamespace, apiResource.getPath(), token, resource, true);

        } catch (HttpClientResponseException e) {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED |@") + apiResource.getKind() + "/" + resource.getMetadata().getName() + Ansi.AUTO.string("@|bold,red  failed with message : |@") + e.getMessage());
            return 1;
        }

        // 3. convert to yaml
        DumperOptions options = new DumperOptions();
        options.setExplicitStart(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Representer representer = new Representer();
        representer.addClassTag(Resource.class, Tag.MAP);
        PrintWriter writer;

        String oldResourceFileName = generateFileName("old", resource.getMetadata().getName());
        String newResourceFileName = generateFileName("new", resource.getMetadata().getName());
        try {
            writer = new PrintWriter(new File(oldResourceFileName));
            new Yaml(representer,options).dump(oldResource, writer);

            writer = new PrintWriter(new File(newResourceFileName));
            new Yaml(representer,options).dump(newResource, writer);
        } catch (FileNotFoundException e) {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED |@") + e.getMessage());
            return 1;
        }

        // 4. call diff on the files created
        Runtime rt = Runtime.getRuntime();
        try {
            //TODO manage other diff function
            Process p = rt.exec("diff -u -N " + oldResourceFileName + " " + newResourceFileName);
            BufferedReader output = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String toPrint = output.lines()
                .collect(Collectors.joining(System.lineSeparator()));

            int exitValue = p.waitFor();
            if (exitValue != 0) {
                System.out.println(toPrint);
            }
        } catch(Exception e) {
            System.out.println(Ansi.AUTO.string("@|bold,red FAILED |@") + e.getMessage());
            return 1;
        }

        // 5. clean files
        new File(oldResourceFileName).delete();
        new File(newResourceFileName).delete();

        return 0;
    }

    private String generateFileName(String prefix, String name) {
        //TODO test and adapt to Windows OS
        return "/tmp/" + prefix + "_" + name + ".yml";
    }
}
