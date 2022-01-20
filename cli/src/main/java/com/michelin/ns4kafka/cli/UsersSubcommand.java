package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ConfigService;
import com.michelin.ns4kafka.cli.services.FormatService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "reset-password", description = "Reset your Kafka password")
public class UsersSubcommand implements Callable<Integer> {
    @Inject
    public ConfigService configService;

    @Inject
    public LoginService loginService;

    @Inject
    public ResourceService resourceService;

    @Inject
    public KafkactlConfig kafkactlConfig;

    @Inject
    public FormatService formatService;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    @CommandLine.Parameters(description = "The user to reset password", arity = "1")
    public String user;

    @CommandLine.Option(names = {"--execute"}, description = "This option is mandatory to change the password")
    public boolean confirmed;

    @CommandLine.Option(names = {"-o", "--output"}, description = "Output format. One of: yaml|table", defaultValue = "table")
    public String output;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    @Override
    public Integer call() throws Exception {
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }
        KafkactlConfig.Context currentContext = configService.getCurrentContext();

        String namespace = kafkactlCommand.optionalNamespace.orElse(currentContext.getContext().getCurrentNamespace());

        if (!List.of("table", "yaml").contains(output)) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Invalid value " + output + " for option -o");
        }

        if (!confirmed) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(),
                    "! WARNING ! WARNING ! WARNING ! WARNING ! WARNING ! WARNING ! WARNING !\n" +
                            "You are about to change your Kafka password for namespace " + namespace + "\n" +
                            "Active connections will be killed instantly\n\n"+
                            "To execute this operation, rerun with option --execute"
            );
        }

        Resource res = resourceService.resetPassword(namespace, user);
        if (res != null) {
            formatService.displaySingle(res, output);
        }

        return 0;
    }
}
