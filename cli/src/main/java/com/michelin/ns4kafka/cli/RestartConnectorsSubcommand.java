package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "restart-connectors", description = "Restart connector deployed on the cluster")
public class RestartConnectorsSubcommand implements Callable<Integer> {

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;
    @CommandLine.Parameters(index = "0", description = "Resource name", arity = "1")
    public String resourceName;
    @CommandLine.Option(names = {"--dry-run"}, description = "Does not restart connect. Validate only")
    public boolean dryRun;

    @Inject
    public LoginService loginService;
    @Inject
    public KafkactlConfig kafkactlConfig;
    @Inject
    public ResourceService resourceService;

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
        if (resourceService.restartConnect(namespace,resourceName,dryRun)){
            System.out.println("Success");
            return 0;
        };
        return 1;

    }
}
