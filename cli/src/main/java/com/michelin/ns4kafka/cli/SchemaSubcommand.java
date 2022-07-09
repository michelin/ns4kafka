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

@CommandLine.Command(name = "schemas", description = "Update schema compatibility mode")
public class SchemaSubcommand implements Callable<Integer> {
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
     * Current command
     */
    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    /**
     * Kafkactl configuration
     */
    @Inject
    public KafkactlConfig kafkactlConfig;

    /**
     * Format service
     */
    @Inject
    public FormatService formatService;

    /**
     * Kafkactl command
     */
    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    /**
     * Compatibility to set
     */
    @CommandLine.Parameters(index="0",  description = "Compatibility mode to set [GLOBAL, BACKWARD, " +
            "BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE]. " +
            "GLOBAL will revert to Schema Registry's compatibility level", arity = "1")
    public SchemaCompatibility compatibility;

    /**
     * List of subjects to update
     */
    @CommandLine.Parameters(index="1..*", description = "Subject names separated by space", arity = "1..*")
    public List<String> subjects;

    /**
     * Run the "reset-offsets" command
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

        return 1;
    }
}
