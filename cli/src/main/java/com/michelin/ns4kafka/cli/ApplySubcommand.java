package com.michelin.ns4kafka.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.client.NonNamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceKind;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "apply" , description = "Create or update a resource")
public class ApplySubcommand extends AbstractJWTCommand implements Callable<Integer>{

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    NonNamespacedResourceClient nonNamespacedClient;

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

    public Integer call() throws Exception {
        if (!json.isBlank()) {
            String token = getJWT();
            token = "Bearer " + token;
            switch(kind) {
            case NAMESPACE:
                nonNamespacedClient.apply(token, json);
                break;
            case ROLEBINDING:
                namespacedClient.apply(namespace, "role-bindings", token, json);
                break;
            case ACCESSCONTROLENTRY:
                namespacedClient.apply(namespace, "acls", token, json);
                break;
            case CONNECTOR:
                namespacedClient.apply(namespace, "connects", token, json);
                break;
            case TOPIC:
                namespacedClient.apply(namespace, "topic", token, json);
                break;
            default:
                throw new Exception();
            }
        }
        return 0;
    }
}
