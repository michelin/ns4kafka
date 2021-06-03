package com.michelin.ns4kafka.cli;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.TopicService;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Help.Ansi;

@Command(name = "empty-topic", description = "Interact with a Topic")
public class TopicSubcommand implements Callable<Integer> {

    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public LoginService loginService;
    @Inject
    public TopicService topicService;

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


        boolean deleted = topicService.empty(namespace, topic, dryRun);
        if (!deleted) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,red FAILED |@"));
            return 1;
        }
        System.out.println(Ansi.AUTO.string("@|bold,green SUCCESS |@"));
        return 0;
    }

}
