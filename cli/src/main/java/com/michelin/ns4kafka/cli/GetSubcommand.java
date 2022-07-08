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
    /**
     * Namespaced resource client
     */
    @Inject
    public NamespacedResourceClient namespacedClient;

    /**
     * Cluster resource client
     */
    @Inject
    public ClusterResourceClient nonNamespacedClient;

    /**
     * Login service
     */
    @Inject
    public LoginService loginService;

    /**
     * API resources service
     */
    @Inject
    public ApiResourcesService apiResourcesService;

    /**
     * Resource service
     */
    @Inject
    public ResourceService resourceService;

    /**
     * Format service
     */
    @Inject
    public FormatService formatService;

    /**
     * Kafkactl configuration
     */
    @Inject
    public KafkactlConfig kafkactlConfig;

    /**
     * Kafkactl command
     */
    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    /**
     * Resource type to get
     */
    @Parameters(index = "0", description = "Resource type or 'all' to display resources for all types", arity = "1")
    public String resourceType;

    /**
     * Resource name to get
     */
    @Parameters(index = "1", description = "Resource name", arity = "0..1")
    public Optional<String> resourceName;

    /**
     * Output format
     */
    @Option(names = {"-o", "--output"}, description = "Output format. One of: yaml|table", defaultValue = "table")
    public String output;

    /**
     * Current command
     */
    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    /**
     * Run the "get" command
     * @return The command return code
     * @throws Exception Any exception during the run
     */
    @Override
    public Integer call() throws Exception {
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        // Validate resourceType + custom type ALL
        List<ApiResource> apiResources = validateResourceType();

        validateOutput();

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        // List resources based on parameters
        if (resourceName.isEmpty() || apiResources.size() > 1) {
            try {
                // List all resources for given types (k get all, k get topics)
                Map<ApiResource, List<Resource>> resources = resourceService.listAll(apiResources, namespace);
                // Display all resources by type
                resources.entrySet()
                        .stream()
                        .filter(kv -> !kv.getValue().isEmpty())
                        .forEach(kv -> formatService.displayList(kv.getKey().getKind(), kv.getValue(), output));
            } catch (HttpClientResponseException e) {
                formatService.displayError(e, apiResources.get(0).getKind(), null);
            } catch (Exception e) {
                System.out.println("Error during get for resource type " + resourceType + ": " + e.getMessage());
            }
        } else {
            try {
                // Get individual resources for given types (k get topic topic1)
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

    /**
     * Validate required output format
     */
    private void validateOutput() {
        if (!List.of("table", "yaml").contains(output)) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Invalid value " + output + " for option -o");
        }
    }

    /**
     * Validate required resource type
     * @return The list of resource type
     */
    private List<ApiResource> validateResourceType() {
        // Specific case ALL
        if (resourceType.equalsIgnoreCase("ALL")) {
            return apiResourcesService.getListResourceDefinition()
                    .stream()
                    .filter(ApiResource::isNamespaced)
                    .collect(Collectors.toList());
        }

        // Otherwise, check resource exists
        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isPresent()) {
            return List.of(optionalApiResource.get());
        }

        throw new CommandLine.ParameterException(commandSpec.commandLine(), "The server doesn't have resource type " + resourceType);
    }
}
