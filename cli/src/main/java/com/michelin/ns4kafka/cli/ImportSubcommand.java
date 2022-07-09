package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.FormatService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
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

@Command(name = "import", description = "Import resources already present on the Kafka Cluster in ns4kafka")
public class ImportSubcommand implements Callable<Integer> {
    /**
     * Login service
     */
    @Inject
    public LoginService loginService;

    /**
     * Resource service
     */
    @Inject
    public ResourceService resourceService;

    /**
     * API resources service
     */
    @Inject
    public ApiResourcesService apiResourcesService;

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
     * Resource type to import
     */
    @Parameters(index = "0", description = "Resource type", arity = "1")
    public String resourceType;

    /**
     * Does not persist resources. Validate only
     */
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    public boolean dryRun;

    /**
     * Current command
     */
    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    /**
     * Run the "get" command
     * @return The command return code
     */
    public Integer call() {
        if (dryRun) {
            System.out.println("Dry run execution");
        }

        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        // Validate resourceType + custom type ALL
        List<ApiResource> apiResources = validateResourceType();

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        Map<ApiResource, List<Resource>> resources = resourceService.importAll(apiResources, namespace, dryRun);

        // Display all resources by type
        resources.forEach((k, v) -> formatService.displayList(k.getKind(), v, "table"));
        return 0;
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
                    .filter(ApiResource::isSynchronizable)
                    .collect(Collectors.toList());
        }

        // Otherwise, check resource exists
        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "The server doesn't have resource type " + resourceType);
        }

        if (!optionalApiResource.get().isSynchronizable()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Resource Type " + resourceType + " is not synchronizable");
        }

        return List.of(optionalApiResource.get());
    }
}
