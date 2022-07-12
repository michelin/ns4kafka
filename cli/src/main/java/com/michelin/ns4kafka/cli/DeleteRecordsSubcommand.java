package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.FormatService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "delete-records", description = "Deletes all records within a topic")
public class DeleteRecordsSubcommand implements Callable<Integer> {
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
     * Topic from which delete records
     */
    @Parameters(description = "Name of the topic", arity = "1")
    public String topic;

    /**
     * Is the command run with dry run mode
     */
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    public boolean dryRun;

    /**
     * Current command
     */
    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    /**
     * Run the "delete-records" command
     * @return The command return code
     * @throws Exception Any exception during the run
     */
    @Override
    public Integer call() throws Exception {
        if (dryRun) {
            System.out.println("Dry run execution");
        }

        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        List<Resource> resources = resourceService.deleteRecords(namespace, topic, dryRun);
        if (!resources.isEmpty()) {
            formatService.displayList("DeleteRecordsResponse", resources, "table");
            return 0;
        }

        return 0;
    }
}
