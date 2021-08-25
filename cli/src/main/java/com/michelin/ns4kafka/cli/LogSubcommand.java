package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.services.AdminService;
import com.michelin.ns4kafka.cli.services.LoginService;
import io.micronaut.core.annotation.Introspected;
import lombok.Builder;
import lombok.Data;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "logs", description = "Get Logs from the API")
public class LogSubcommand implements Callable<Integer> {

    @Option(names = {"--by-duration"}, description = "Get logs from a duration format [ PnDTnHnMnS ]")
    public Optional<Duration> duration;

    @Inject
    public AdminService adminService;
    @Inject
    public LoginService loginService;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    @Override
    public Integer call() throws Exception {
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }
        if (duration.isPresent()) {
            FormatUtils.displayLogs(adminService.retrievesLogsFromDuration(duration.get()));
            return 0;
        }
        FormatUtils.displayLogs(adminService.defaultRetrievesLogs());
        return 0;
    }


    @Builder
    @Data
    @Introspected
    public static class AuditLog {

        private String user;
        private String date;
        private Long timestamp;
        private String kind;
        private ObjectMeta metadata;
        private String operation;
        private String before;
        private String after;
    }
}
