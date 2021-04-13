package com.michelin.ns4kafka.cli;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.client.NonNamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ResourceKind;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "list" , description = "List all resources of a Namespace")
public class CommandList extends AbstractJWTCommand implements Callable<Integer>{

    @Inject
    NamespacedResourceClient namespacedClient;

    @Inject
    NonNamespacedResourceClient nonNamespacedClient;

    @Option(names = {"-n", "--namespace"})
    String namespace;

    @Option(names = {"-k", "--kind"}, description = "The kind which you want the list", required = true)
    String kind;

    @Override
    public Integer call() throws Exception {
        String token = getJWT();
        token = "Bearer " + token;
        String namespaceValue = namespace;
        ResourceKind resourceKind = ResourceKind.resourceKindFromValue(kind);
        switch(resourceKind) {
        case NAMESPACE:
            nonNamespacedClient.list();
            break;
        case ROLEBINDING:
            namespacedClient.list(namespaceValue, "role-bindings", token);
            break;
        case ACCESSCONTROLENTRY:
            namespacedClient.list(namespaceValue, "acls", token);
            break;
        case CONNECTOR:
            namespacedClient.list(namespaceValue, "connects", token);
            break;
        case TOPIC:
            namespacedClient.list(namespaceValue, "topic", token);
            break;
        default:
            throw new Exception();
        }
        return 0;
    }
}
