package com.michelin.ns4kafka.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.client.NonNamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Help.Ansi;

@Command(name = "apply" , description = "Create or update a resource")
public class ApplySubcommand extends AbstractJWTCommand implements Callable<Integer>{

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    NonNamespacedResourceClient nonNamespacedClient;

    String json = "";
    String kind;
    String namespace;
    @Option(names = {"-f", "--file"}, required = true, description = "File in Yaml describing the system Kafka")
    public void convertYamlToJson(File file) throws Exception {
        //TODO better management of Exception
        Yaml yaml = new Yaml(new Constructor(Resource.class));
        ObjectMapper jsonWriter = new ObjectMapper();
        InputStream inputStream = new FileInputStream(file);

        //TODO manage multi document YAML
        Resource resourceYaml = yaml.load(inputStream);

        kind = resourceYaml.getKind();
        namespace = resourceYaml.getMetadata().getNamespace();

        //convert to JSON
        json = jsonWriter.writeValueAsString(resourceYaml);

    }

    public Integer call() {
        String token = getJWT();
        token = "Bearer " + token;
        Optional<ResourceDefinition> optionalResourceDefinition = manageResource.getResourceDefinitionFromKind(kind);
        ResourceDefinition resourceDefinition;
        try {
           resourceDefinition = optionalResourceDefinition.get();
        } catch(Exception e) {
            System.out.println(Ansi.AUTO.string("@|bold,red Can't find the resource's kind: |@") + kind);
            return 2;
        }
        try {
            if(resourceDefinition.isNamespaced()) {
                namespacedClient.apply(namespace, resourceDefinition.getPath(), token, json);
            }
            else {
                nonNamespacedClient.apply(token, json);
            }
        } catch(HttpClientResponseException e) {
            HttpStatus status = e.getStatus();
            switch(status){
                case BAD_REQUEST:
                    System.err.println(e.getMessage());
                    return 2;
                default:
                System.err.println(Ansi.AUTO.string("@|bold,red Resource failed with message : |@")+e.getMessage());
            }
            return 1;
        }
        System.out.println(resourceDefinition.getKind() + ": " + Ansi.AUTO.string("@|bold,green SUCCESS|@"));
        return 0;
    }
}
