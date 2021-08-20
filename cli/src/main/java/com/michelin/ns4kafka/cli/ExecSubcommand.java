package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ActionDefinition;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "exec", description = "Execute action on resources")
public class ExecSubcommand implements Callable<Integer> {

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;
    @CommandLine.Parameters(index = "0", description = "Resource type", arity = "1")
    public String resourceType;
    @CommandLine.Parameters(index = "1", description = "Action name", arity = "1")
    public String actionName;
    @CommandLine.Parameters(index = "2", description = "Resource name", arity = "0..1")
    public Optional<String> resourceName;
    @CommandLine.Option(names = {"--dry-run"}, description = "Does not execute action. Validate only")
    public boolean dryRun;

    @Inject
    public LoginService loginService;
    @Inject
    public ApiResourcesService apiResourcesService;
    @Inject
    public KafkactlConfig kafkactlConfig;
    @Inject
    public ResourceService resourceService;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    @Override
    public Integer call() throws Exception {

        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        // 1. validate resource types
        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "The server doesn't have resource type " + resourceType);
        }
        ApiResource apiResource = optionalApiResource.get();

        // 2. get Action available for the resource type
        List<ActionDefinition> actionDefinitions = apiResourcesService.getListActionDefinition(apiResource.getKind());

        // 3. get Action designed by the name
        Optional<ActionDefinition> optionalActionDefinition = actionDefinitions.stream()
                .filter(definition -> definition.getNames().equals(actionName))
                .findFirst();
        if (optionalActionDefinition.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "The server doesn't have action " + actionName + " for resource type "+ resourceType);
        }
        var actionDefinition = optionalActionDefinition.get();

        // 4. get if action must have resource;
        if (resourceName.isEmpty() && actionDefinition.isMustHaveResourceName()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "The action " + actionName + " for resource type "+ resourceType + " dpends of a resource name");
        }

        // 5. execute action
        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());
        resourceService.action(namespace,apiResource,actionDefinition,resourceName.get(),dryRun);

        return 0;
    }
}
