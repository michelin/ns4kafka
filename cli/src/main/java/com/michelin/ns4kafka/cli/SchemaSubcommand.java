package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.SchemaCompatibility;
import com.michelin.ns4kafka.cli.services.FormatService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "schemas", description = "Interact with schemas (Config)")
public class SchemaSubcommand implements Callable<Integer> {
    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    @CommandLine.Parameters(index = "0", description = "(config)", arity = "1")
    public SchemaAction action;

    @CommandLine.Parameters(index="1", description = "Compatibility mode to apply", arity = "1")
    public SchemaCompatibility compatibility;

    @CommandLine.Parameters(index="2..*", description = "Subject names separated by space", arity = "1..*")
    public List<String> subjects;

    @Inject
    public LoginService loginService;

    @Inject
    public ResourceService resourceService;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public FormatService formatService;

    @Override
    public Integer call() throws Exception {
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        if (action.equals(SchemaAction.config)) {
            List<Resource> updatedSchemas = subjects
                    .stream()
                    .map(subject -> this.resourceService.changeSchemaCompatibility(namespace, subject,
                            compatibility))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (!updatedSchemas.isEmpty()) {
                formatService.displayList("SchemaCompatibilityState", updatedSchemas, "table");
                return 0;
            }
        }

        return 1;
    }
}

enum SchemaAction {
    config
}
