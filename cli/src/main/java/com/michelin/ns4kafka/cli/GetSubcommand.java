package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.FormatService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Command(name = "get", description = {
        "Get resources by resource type for the current namespace",
        "Examples:",
        "  kafkactl get topic topic1 : Display topic1 configuration",
        "  kafkactl get topics : Display all topics",
        "  kafkactl get all : Display all resources",
        "Parameters: "
})
public class GetSubcommand implements Callable<Integer> {

    @Inject
    public NamespacedResourceClient namespacedClient;
    @Inject
    public ClusterResourceClient nonNamespacedClient;

    @Inject
    public LoginService loginService;
    @Inject
    public ApiResourcesService apiResourcesService;
    @Inject
    public ResourceService resourceService;
    @Inject
    public FormatService formatService;
    @Inject
    public KafkactlConfig kafkactlConfig;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;
    @Parameters(index = "0", description = "Resource type or 'all' to display resources for all types", arity = "1")
    public String resourceType;
    @Parameters(index = "1", description = "Resource name", arity = "0..1")
    public Optional<String> resourceName;
    @Option(names = {"-o", "--output"}, description = "Output format. One of: yaml|table", defaultValue = "table")
    public String output;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    @Override
    public Integer call() throws Exception {

        // 1. Authent
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        // 2. validate resourceType + custom type ALL
        List<ApiResource> apiResources = validateResourceType();

        validateOutput();

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());
        // 3. list resources based on parameters
        if (resourceName.isEmpty() || apiResources.size() > 1) {
            try {
                // 4.a list all resources for given types (k get all, k get topics)
                Map<ApiResource, List<Resource>> resources = resourceService.listAll(apiResources, namespace);
                // 5.a display all resources by type
                resources.forEach((k, v) -> formatService.displayList(k.getKind(), v, output));
            } catch (HttpClientResponseException e) {
                formatService.displayError(e, apiResources.get(0).getKind(), null);
            } catch (Exception e) {
                System.out.println("Error during get for resource type " + resourceType + ": " + e.getMessage());
            }
        } else {

            try {
                // 4.b get individual resources for given types (k get topic topic1)
                Resource singleResource = resourceService.getSingleResourceWithType(apiResources.get(0), namespace, resourceName.get(), true);
                formatService.displaySingle(singleResource, output);
            } catch (HttpClientResponseException e) {
                formatService.displayError(e, apiResources.get(0).getKind(), resourceName.get());
            } catch (Exception e) {
                System.out.println("Error during get for resource type " + apiResources.get(0).getKind() + "/" + resourceName.get() + ": " + e.getMessage());
            }

        }

        return 0;
    }

    private void validateOutput() {
        if (!List.of("table", "yaml").contains(output)) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Invalid value " + output + " for option -o");
        }
    }

    private List<ApiResource> validateResourceType() {
        // specific case ALL
        if (resourceType.equalsIgnoreCase("ALL")) {
            return apiResourcesService.getListResourceDefinition()
                    .stream()
                    .filter(apiResource -> apiResource.isNamespaced())
                    .collect(Collectors.toList());
        }
        // otherwise check resource exists
        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isPresent()) {
            return List.of(optionalApiResource.get());
        }
        throw new CommandLine.ParameterException(commandSpec.commandLine(), "The server doesn't have resource type " + resourceType);

    }

}
