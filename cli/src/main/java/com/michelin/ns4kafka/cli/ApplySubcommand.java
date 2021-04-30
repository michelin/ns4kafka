package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Command(name = "apply" , description = "Create or update a resource")
public class ApplySubcommand extends AbstractJWTCommand implements Callable<Integer>{

    @CommandLine.ParentCommand // picocli injects the parent instance
    private KafkactlCommand parentCommand;

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    ClusterResourceClient nonNamespacedClient;

    @Option(names = {"-f", "--file"}, required = true, description = "Files in Yaml describing the system Kafka")
    File file;

    @Option(names = {"-r", "--recursive"}, description = "Enable recursive search of file")
    boolean recursive;

    @Override
    public Integer call() throws Exception {

        // 1. list all files to process
        List<File> yamlFiles = listAllFiles(new File[] { file }, recursive);
        if(yamlFiles.isEmpty()){
            System.out.println("Could not find files in "+file.getName());
            return 1;
        }
        //yamlFiles.forEach(file1 -> System.out.println(file1.getParent()+"/"+file1.getName()));
        // 2. load all files into Resource Descriptors objects
        List<Resource> resources = loadAllResources(yamlFiles);
        resources.forEach(resource -> System.out.println(resource.getMetadata().getName()));

        // 2. process each document individually
        resources.forEach(resource -> applyResource(resource));

        return 0;
    }

    private int applyResource(Resource resource) {
        String token = getJWT();
        token = "Bearer " + token;

        Optional<ResourceDefinition> optionalResourceDefinition = manageResource.getResourceDefinitionFromKind(resource.getKind());
        ResourceDefinition resourceDefinition = null;
        try {
            resourceDefinition = optionalResourceDefinition.get();
            if(resourceDefinition.isNamespaced()) {
                namespacedClient.apply(resource.getMetadata().getNamespace(), resourceDefinition.getPath(), token, resource);
            } else {
                nonNamespacedClient.apply(token, resourceDefinition.getPath(), resource);
            }
            System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS: |@") + resourceDefinition.getKind() + "/" + resource.getMetadata().getName());

        } catch(NoSuchElementException e) {
            System.out.println(Ansi.AUTO.string("@|bold,red Can't find the resource's kind: |@") + resource.getKind());

        } catch(HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch(status){
                case UNAUTHORIZED:
                    System.out.println(Ansi.AUTO.string("@|bold,red Resource |@") + resourceDefinition.getKind() + "/" + resource.getMetadata().getName() + Ansi.AUTO.string("@|bold,red  failed with message : |@") + e.getMessage());
                    System.out.println("Please login first");
                    break;
                default:
                    System.out.println(Ansi.AUTO.string("@|bold,red Resource |@") + resourceDefinition.getKind() + "/" + resource.getMetadata().getName() + Ansi.AUTO.string("@|bold,red  failed with message : |@") + e.getMessage());
            }
        }
        return 0;
    }

    private List<Resource> loadAllResources(List<File> yamlFiles) {
        Yaml yaml = new Yaml(new Constructor(Resource.class));
        return yamlFiles
                .stream()
                .map(file -> {
                    try{
                        return new FileInputStream(file);
                    } catch (FileNotFoundException e) {
                        System.out.println(e.getMessage());
                        return null;
                    }
                })
                .map(fileInputStream ->  yaml.loadAll(fileInputStream))
                .flatMap(objects -> StreamSupport.stream(objects.spliterator(), false))
                .map(o -> (Resource)o)
                .collect(Collectors.toList());
    }

    private List<File> listAllFiles(File[] rootDir, boolean recursive) {
        List<File> allFiles = new ArrayList<>();
        for (File currentElement : rootDir) {
            // if the file is a directory, look for the files inside it
            if (currentElement.isDirectory()) {
                // files
                File[] currentFiles = currentElement.listFiles(file -> file.isFile() && (file.getName().endsWith(".yaml") || file.getName().endsWith(".yml")));
                allFiles.addAll(Arrays.asList(currentFiles));
                // folders
                if(recursive) {
                    File[] currentDirs = currentElement.listFiles(file -> file.isDirectory());
                    allFiles.addAll(listAllFiles(currentDirs, true));
                }

            } else {
                allFiles.add(currentElement);
            }
        }
        return allFiles;

    }
}
