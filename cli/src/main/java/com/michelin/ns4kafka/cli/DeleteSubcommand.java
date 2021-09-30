package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "delete", description = "Delete a resource")
public class DeleteSubcommand implements Callable<Integer> {
    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public ResourceService resourceService;

    @Inject
    public LoginService loginService;

    @Inject
    public ApiResourcesService apiResourcesService;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    @Parameters(index = "0", description = "Resource type", arity = "1")
    public String resourceType;

    @Parameters(index = "1", description = "Resource name", arity = "1")
    public String name;

    @Option(names = {"--dry-run"}, description = "Does not persist operation. Validate only")
    public boolean dryRun;

    /**
     * Option to perform a hard delete
     */
    @Option(names = {"--hard"}, description = "Perform a hard delete of the resource")
    public boolean hard;

    @Override
    public Integer call() {
        if (dryRun) {
            System.out.println("Dry run execution");
        }

        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "The server doesn't have resource type [" + resourceType + "]");
        }

        ApiResource apiResource = optionalApiResource.get();

        boolean deleted = resourceService.delete(apiResource, namespace,
                name, dryRun, hard);

        if (deleted) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,green Success |@") + resourceType + "/" + name + " (deleted)");

            return 0;
        }

        return 1;
    }

}
