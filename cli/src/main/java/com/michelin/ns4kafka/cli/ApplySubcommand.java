package com.michelin.ns4kafka.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "apply" , description = "Create or update a resource")
public class ApplySubcommand extends AbstractJWTCommand implements Callable<Integer>{

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    ClusterResourceClient nonNamespacedClient;

    @Option(names = {"-f", "--file"}, required = true, description = "Files in Yaml describing the system Kafka")
    File[] files;

    @Option(names = {"-r", "--recursive"}, description = "Enable recursive search of file")
    boolean recursive;

    private void sendJsonToAPI(JsonNode jsonNode) {
        String token = getJWT();
        token = "Bearer " + token;

        String kind = jsonNode.get("kind").textValue();
        String name = jsonNode.get("metadata").get("name").textValue();
        String namespace = jsonNode.get("metadata").get("namespace").textValue();
        String json = jsonNode.toString();

        Optional<ResourceDefinition> optionalResourceDefinition = manageResource.getResourceDefinitionFromKind(kind);
        ResourceDefinition resourceDefinition = null;
        try {
            resourceDefinition = optionalResourceDefinition.get();
            if(resourceDefinition.isNamespaced()) {
                namespacedClient.apply(namespace, resourceDefinition.getPath(), token, json);
            } else {
                nonNamespacedClient.apply(token, resourceDefinition.getPath(), json);
            }
            System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS: |@") + resourceDefinition.getKind() + "/" + name);

        } catch(NoSuchElementException e) {
            System.out.println(Ansi.AUTO.string("@|bold,red Can't find the resource's kind: |@") + kind);

        } catch(HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch(status){
                case UNAUTHORIZED:
                    System.out.println(Ansi.AUTO.string("@|bold,red Resource |@") + resourceDefinition.getKind() + "/" + name + Ansi.AUTO.string("@|bold,red  failed with message : |@") + e.getMessage());
                    System.out.println("Please login first");
                    break;
                default:
                    System.out.println(Ansi.AUTO.string("@|bold,red Resource |@") + resourceDefinition.getKind() + "/" + name + Ansi.AUTO.string("@|bold,red  failed with message : |@") + e.getMessage());
            }
        }
    }

    private void convertYamlToJson(File file) {
        Yaml yaml = new Yaml(new Constructor(Resource.class));
        ObjectMapper mapper = new ObjectMapper();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            Iterable<Object> yamlResources = yaml.loadAll(inputStream);
            //convert to JSON
            for (Object yamlResource : yamlResources) {
                //sendJson
                sendJsonToAPI(mapper.valueToTree(yamlResource));
            }
        } catch (FileNotFoundException e) {
            System.out.println(Ansi.AUTO.string("@|bold,red Can't find file: |@") + file.getPath());
        }
    }

    private void identifyFileFormat(File file) {
        if (file.getName().endsWith(".yml") || file.getName().endsWith(".yaml")) {
            convertYamlToJson(file);
        } else if (file.getName().endsWith(".json")) {
            //TODO Implements json file

        }

    }

    private void recursiveDirectory (File fileDirectory) {
        File[] filesFromdirectory = fileDirectory.listFiles();
        for (File file : filesFromdirectory) {
            if (file.isDirectory()) {
                recursiveDirectory(file);
            } else {
                identifyFileFormat(file);
            }

        }
    }

    @Override
    public Integer call() throws Exception {
        // look for each file described by the option f
        for (File file : files) {
            // if the file is a directory, look for the files inside it
            if (file.isDirectory()) {
                File[] filesFromdirectory = file.listFiles();
                for (File file2 : filesFromdirectory) {
                    // if option recursive is enable, look for file inside the directory
                    if (recursive && file2.isDirectory()) {
                        recursiveDirectory(file2);
                    } else {
                        identifyFileFormat(file2);
                    }
                }

            } else {
                identifyFileFormat(file);
            }
        }
        return 0;
    }
}
