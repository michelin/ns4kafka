package com.michelin.ns4kafka.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceKind;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import io.micronaut.configuration.picocli.PicocliRunner;
import io.micronaut.context.ApplicationContext;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "kafkactl", description = "...",
        mixinStandardHelpOptions = true)
public class KafkactlCommand implements Runnable {

    @Option(names = {"-v", "--verbose"}, description = "...")
    boolean verbose;

    @Option(names = {"-f", "--file"}, description = "File in Yaml describing the system Kafka")
    File file;

    public static void main(String[] args) throws Exception {
        PicocliRunner.run(KafkactlCommand.class, args);
    }

    public void run() {
        // business logic here
        System.out.println("Hi!");
        if (verbose) {
            System.out.println("Hi!");
        }
        if (file.exists()) {
            Yaml yaml = new Yaml(new Constructor(Resource.class));
            ObjectMapper jsonWriter = new ObjectMapper();
            String json;
            try {
                InputStream inputStream = new FileInputStream(file);
                //managed multi document YAML
                for (Object resourceYaml : yaml.loadAll(inputStream)) {
                    // TODO convert object to Resource to remove this line
                    Resource resource = new Resource();
                    //Throws exception if the kind doesn't exist
                    ResourceKind kind = ResourceKind.valueOf(resource.getKind());
                    json = jsonWriter.writeValueAsString(resourceYaml);
                    switch(kind) {
                    case NAMESPACE:
                        HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("/api/namespaces"))
                            .setHeader("Content-Type", "application/json")
                            .POST(BodyPublishers.ofString(json))
                            .build();
                        break;
                    case ACCESSCONTROLENTRY:
                        break;
                    case CONNECTOR:
                        break;
                    case TOPIC:
                        break;
                    default:
                        break;
                    }
                }

            }
            catch (Exception e) {
                System.out.print(e);
            }

        }
    }
}
