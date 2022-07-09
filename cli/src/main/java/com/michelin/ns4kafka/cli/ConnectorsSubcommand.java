package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ApiResourcesService;
import com.michelin.ns4kafka.cli.services.FormatService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "connectors", description = "Interact with connectors (Pause/Resume/Restart)")
public class ConnectorsSubcommand implements Callable<Integer> {
    /**
     * Kafkactl command
     */
    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    /**
     * Action to perform on connectors
     */
    @CommandLine.Parameters(index = "0", description = "(pause | resume | restart)", arity = "1")
    public ConnectorAction action;

    /**
     * List of connectors
     */
    @CommandLine.Parameters(index="1..*", description = "Connector names separated by space (use `ALL` for all connectors)", arity = "1..*")
    public List<String> connectors;

    /**
     * Login service
     */
    @Inject
    public LoginService loginService;

    /**
     * Kafkactl configuration
     */
    @Inject
    public KafkactlConfig kafkactlConfig;

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
     * Current command
     */
    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    /**
     * Run the "connectors" command
     * @return The command return code
     * @throws Exception Any exception during the run
     */
    @Override
    public Integer call() throws Exception {
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        if (connectors.stream().anyMatch(s -> s.equalsIgnoreCase("ALL"))) {
            ApiResource connectType = apiResourcesService.getResourceDefinitionFromKind("Connector")
                    .orElseThrow(() -> new CommandLine.ParameterException(commandSpec.commandLine(), "`Connector` Kind not found in ApiResources Service"));
            connectors = resourceService.listResourcesWithType(connectType, namespace)
                    .stream()
                    .map(resource -> resource.getMetadata().getName())
                    .collect(Collectors.toList());
        }

        List<Resource> changeConnectorResponseList = connectors.stream()
                // Prepare request object
                .map(connector -> Resource.builder()
                        .metadata(ObjectMeta.builder()
                                .namespace(namespace)
                                .name(connector)
                                .build())
                        .spec(Map.of("action", action.toString()))
                        .build())
                .map(changeConnectorStateRequest -> resourceService.changeConnectorState(namespace, changeConnectorStateRequest.getMetadata().getName(), changeConnectorStateRequest))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (!changeConnectorResponseList.isEmpty()) {
            formatService.displayList("ChangeConnectorState", changeConnectorResponseList, "table");
            return 0;
        }

        return 1;
    }
}

enum ConnectorAction {
    pause,
    resume,
    restart
}
