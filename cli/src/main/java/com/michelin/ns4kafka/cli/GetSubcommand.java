package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.client.ClusterResourceClient;
import com.michelin.ns4kafka.cli.client.NamespacedResourceClient;
import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import org.ocpsoft.prettytime.PrettyTime;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.List;
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
            // 4.a list all resources for given types (k get all, k get topics)
            List<Resource> resources = resourceService.listAll(apiResources, namespace);

            // 5.a display all resources by type
            apiResources.forEach(apiResource -> {
                        if (output.equals("yaml")) {
                            resources.stream().forEach(this::displayIndividual);
                        } else {
                            displayAsTable(apiResource,
                                    resources.stream()
                                            .filter(resource -> resource.getKind().equals(apiResource.getKind()))
                                            .collect(Collectors.toList())
                            );
                        }
                    }
            );
        } else {
            // 4.b get individual resources for given types (k get topic topic1)
            Resource singleResource = resourceService.getSingleResourceWithType(apiResources.get(0), namespace, resourceName.get());

            // 5.b display individual resource
            //displayAsTable(apiResources.get(0),resources);
            displayIndividual(singleResource);
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

    private void displayIndividual(Resource resource) {
        DumperOptions options = new DumperOptions();
        options.setExplicitStart(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Representer representer = new Representer();
        representer.addClassTag(Resource.class, Tag.MAP);
        System.out.println(new Yaml(representer, options).dump(resource));
    }


}
