package com.michelin.ns4kafka.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.client.NonNamespacedResourceClient;
import com.michelin.ns4kafka.cli.client.ResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceKind;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import io.micronaut.configuration.picocli.PicocliRunner;
import io.micronaut.core.io.IOUtils;
import io.micronaut.http.HttpRequest;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

//TODO Make subcommand understand parameters
@Command(name = "kafkactl", subcommands = {Login.class} , description = "...",
        mixinStandardHelpOptions = true)
public class KafkactlCommand implements Callable<Integer> {

    @Inject
    ResourceClient httpClient;

    @Inject
    NonNamespacedResourceClient nonNamespacedClient;

    @Option(names = {"-v", "--verbose"}, description = "...")
    boolean verbose;

    String json = "";
    ResourceKind kind;
    String namespace;
    @Option(names = {"-f", "--file"}, description = "File in Yaml describing the system Kafka")
    public void convertYamlToJson(File file) throws Exception {
        //TODO better management of Exception
        Yaml yaml = new Yaml(new Constructor(Resource.class));
        ObjectMapper jsonWriter = new ObjectMapper();
        InputStream inputStream = new FileInputStream(file);

        //TODO manage multi document YAML
        Resource resourceYaml = yaml.load(inputStream);

        //Throws exception if the kind doesn't exist
        kind = ResourceKind.resourceKindFromValue(resourceYaml.getKind());
        namespace = resourceYaml.getMetadata().getNamespace();

        //convert to JSON
        json = jsonWriter.writeValueAsString(resourceYaml);

    }

    public static void main(String[] args) throws Exception {
        int exitCode = PicocliRunner.execute(KafkactlCommand.class, args);
        System.exit(exitCode);
    }

    public Integer call() throws Exception {
        // business logic here
        System.out.println("Hi!");
        if (verbose) {
            System.out.println("Hi!");
        }
        if (!json.isBlank()) {
            BufferedReader in
            = new BufferedReader(new FileReader("jwt"));
            String token = IOUtils.readText(in);
            token = "Bearer " + token;
            switch(kind) {
            case NAMESPACE:
                nonNamespacedClient.apply(token, json);
                break;
            case ACCESSCONTROLENTRY:
                httpClient.apply(namespace, "acls", token, json);
                break;
            case CONNECTOR:
                httpClient.apply(namespace, "connects", token, json);
                break;
            case TOPIC:
                httpClient.apply(namespace, "topic", token, json);
                break;
            default:
                throw new Exception();
            }
        }
        return 0;

    }

}
