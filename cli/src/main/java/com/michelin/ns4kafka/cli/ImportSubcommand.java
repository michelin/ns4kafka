package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import org.ocpsoft.prettytime.PrettyTime;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Command(name = "import", description = "Import resources already present on the Kafka Cluster in ns4kafka")
public class ImportSubcommand implements Callable<Integer> {

    @Inject
    public LoginService loginService;
    @Inject
    public ResourceService resourceService;
    @Inject
    public ApiResourcesService apiResourcesService;
    @Inject
    public KafkactlConfig kafkactlConfig;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;
    @Parameters(index = "0", description = "Resource type", arity = "1")
    public String resourceType;
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    public boolean dryRun;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    public Integer call() {
        
        if (dryRun) {
            System.out.println("Dry run execution");
        }
        // 1. Authent
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        // 2. validate resourceType + custom type ALL
        List<ApiResource> apiResources = validateResourceType();

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        List<Resource> resources = resourceService.importAll(apiResources, namespace, dryRun);

        // 5.a display all resources by type
        apiResources.forEach(apiResource ->
                displayAsTable(apiResource,
                        resources.stream()
                                .filter(resource -> resource.getKind().equals(apiResource.getKind()))
                                .collect(Collectors.toList())
                )
        );
        return 0;

    }

    private List<ApiResource> validateResourceType() {
        // specific case ALL
        if (resourceType.equalsIgnoreCase("ALL")) {
            return apiResourcesService.getListResourceDefinition()
                    .stream()
                    .filter(apiResource -> apiResource.isSynchronizable())
                    .collect(Collectors.toList());
        }
        // otherwise check resource exists
        Optional<ApiResource> optionalApiResource = apiResourcesService.getResourceDefinitionFromCommandName(resourceType);
        if (optionalApiResource.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "The server doesn't have resource type " + resourceType);
        }
        if(!optionalApiResource.get().isSynchronizable()){
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Resource Type " + resourceType+" is not synchronizable");
        }

        return List.of(optionalApiResource.get());
    }

    private void displayAsTable(ApiResource apiResource, List<Resource> resources) {
        CommandLine.Help.TextTable tt = CommandLine.Help.TextTable.forColumns(
                CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO),
                new CommandLine.Help.Column[]
                        {
                                new CommandLine.Help.Column(50, 2, CommandLine.Help.Column.Overflow.SPAN),
                                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN)
                        });
        tt.addRowValues(apiResource.getKind(), "AGE");
        resources.forEach(resource -> tt.addRowValues(resource.getMetadata().getName(), new PrettyTime().format(resource.getMetadata().getCreationTimestamp())));
        System.out.println(tt);
    }
}
