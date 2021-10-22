package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.FormatService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import lombok.AllArgsConstructor;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "schemas", description = "Interact with schemas (Compat)")
public class SchemaSubcommand implements Callable<Integer> {
    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    @CommandLine.Parameters(index = "0", description = "(compat)", arity = "1")
    public SchemaAction action;

    @CommandLine.Parameters(index="1", description = "Compatibility mode to apply", arity = "1")
    public String mode;

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

        Optional<CompatibilityMode> compatibilityModeOptional = Arrays.stream(CompatibilityMode.values())
                .filter(compatibilityMode -> compatibilityMode.labels.contains(mode))
                .findAny();

        if (compatibilityModeOptional.isEmpty()) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    "Wrong compatibility mode " + mode + ". Expected one of the values " + Arrays.toString(CompatibilityMode.values()));
        }

        if (action.equals(SchemaAction.compat)) {
            List<Resource> updatedSchemas = subjects
                    .stream()
                    .map(subject -> Resource.builder()
                            .metadata(ObjectMeta.builder()
                                    .namespace(namespace)
                                    .name(subject)
                                    .build())
                            .spec(Map.of("compatibility", compatibilityModeOptional.get()))
                            .build())
                    .map(schema -> this.resourceService.changeSchemaCompatibility(namespace, schema.getMetadata().getName(),
                            schema))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (!updatedSchemas.isEmpty()) {
                formatService.displayList("Schema", updatedSchemas, "table");
                return 0;
            }
        }

        return 1;
    }
}

enum SchemaAction {
    compat
}

@AllArgsConstructor
enum CompatibilityMode {
    GLOBAL(Arrays.asList("global", "gl")),
    BACKWARD(Arrays.asList("backward", "ba")),
    BACKWARD_TRANSITIVE(Arrays.asList("backward-transitive", "bat")),
    FORWARD(Arrays.asList("forward", "fo")),
    FORWARD_TRANSITIVE(Arrays.asList("forward-transitive", "fot")),
    FULL(Arrays.asList("full", "fu")),
    FULL_TRANSITIVE(Arrays.asList("full-transitive", "fut")),
    NONE(Arrays.asList("none", "no"));

    List<String> labels;

    @Override
    public String toString() {
        return labels.toString();
    }
}
