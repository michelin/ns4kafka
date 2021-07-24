package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import javax.inject.Inject;
import java.util.concurrent.Callable;

@Command(name = "delete-records", description = "Deletes all records within a topic")
public class DeleteRecordsSubcommand implements Callable<Integer> {

    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public LoginService loginService;
    @Inject
    public ResourceService resourceService;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;
    @Parameters(description = "Name of the topic", arity = "1")
    public String topic;
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    public boolean dryRun;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

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

        Resource resource = resourceService.deleteRecords(namespace, topic, dryRun);

        FormatUtils.displaySingle(null, resource, "yaml");

        return 0;
    }

}
