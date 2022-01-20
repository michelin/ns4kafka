package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.*;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "connectors", description = "Interact with connectors (Pause/Resume/Restart)")
public class ConnectorsSubcommand implements Callable<Integer> {
    @Inject
    public ConfigService configService;

    @Inject
    public LoginService loginService;

    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public ResourceService resourceService;

    @Inject
    public ApiResourcesService apiResourcesService;

    @Inject
    public FormatService formatService;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    @CommandLine.Parameters(index = "0", description = "(pause | resume | restart)", arity = "1")
    public ConnectorAction action;

    @CommandLine.Parameters(index="1..*", description = "Connector names separated by space (use `ALL` for all connectors)", arity = "1..*")
    public List<String> connectors;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    @Override
    public Integer call() throws Exception {
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }
        KafkactlConfig.Context currentContext = configService.getCurrentContextInfos();

        String namespace = kafkactlCommand.optionalNamespace.orElse(currentContext.getContext().getCurrentNamespace());

        // specific case ALL
        if(connectors.stream().anyMatch(s -> s.equalsIgnoreCase("ALL"))){
            ApiResource connectType = apiResourcesService.getResourceDefinitionFromKind("Connector")
                    .orElseThrow(() -> new CommandLine.ParameterException(commandSpec.commandLine(), "`Connector` Kind not found in ApiResources Service"));
            connectors = resourceService.listResourcesWithType(connectType, namespace)
                    .stream()
                    .map(resource -> resource.getMetadata().getName())
                    .collect(Collectors.toList());
        }

        List<Resource> changeConnectorResponseList = connectors.stream()
                // prepare request object
                .map(connector -> Resource.builder()
                        .metadata(ObjectMeta.builder()
                                .namespace(namespace)
                                .name(connector)
                                .build())
                        .spec(Map.of("action", action.toString()))
                        .build())
                // execute action on each connectors
                .map(changeConnectorStateRequest -> resourceService.changeConnectorState(namespace, changeConnectorStateRequest.getMetadata().getName(), changeConnectorStateRequest))
                // drop nulls
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
