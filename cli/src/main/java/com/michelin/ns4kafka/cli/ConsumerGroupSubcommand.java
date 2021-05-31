package com.michelin.ns4kafka.cli;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ConsumerGroupService;
import com.michelin.ns4kafka.cli.services.LoginService;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "consumer-group", description = "Interact with consumer group")
public class ConsumerGroupSubcommand implements Callable<Integer> {

    @Inject
    public LoginService loginService;
    @Inject
    public ConsumerGroupService consumerGroupService;

    @Inject
    public KafkactlConfig kafkactlConfig;

    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;
    @Option(names = {"--group"}, required = true, description = "Consumer group name")
    public String group;
    @Option(names = {"--topic"}, required = true, description = "Topic name to change offset")
    public String topic;
    @Option(names = {"--to-earliest"}, description = "Set offset to its earliest value")
    public boolean earliest;
    @Option(names = {"--to-datetime"}, description = "Set offset to a specific date time in 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss.fffffffff' formats")
    public Timestamp dateTime;
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    public boolean dryRun;

    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    @Override
    public Integer call() throws Exception {

        if (dryRun) {
            System.out.println("Dry run execution");
        }
        if (!earliest && (dateTime == null)) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Should specify --to-earliest or --to-datetime");
        }
        if (earliest && (dateTime != null)) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Should specify only one method (--to-earliest or --to-datetime)");
        }
        boolean authenticated = loginService.doAuthenticate();
        if (!authenticated) {
            throw new CommandLine.ParameterException(commandSpec.commandLine(), "Login failed");
        }

        String namespace = kafkactlCommand.optionalNamespace.orElse(kafkactlConfig.getCurrentNamespace());

        Map<String, Object> consumerGroupResetOffsetSpec = new HashMap<>();
        consumerGroupResetOffsetSpec.put("topic", topic);
        if (earliest) {
            consumerGroupResetOffsetSpec.put("method", "TO_EARLIEST");
        }
        if (dateTime != null) {
            consumerGroupResetOffsetSpec.put("method", "TO_DATETIME");
            consumerGroupResetOffsetSpec.put("timestamp", dateTime.getTime());
        }

        Resource consumerGroupResetOffset = Resource.builder()
            .apiVersion("v1")
            .kind("ConsumerGroupResetOffset")
            .metadata(ObjectMeta.builder()
                    .namespace(namespace)
                    .build())
            .spec(consumerGroupResetOffsetSpec)
            .build();

        Resource result;

        if (earliest) {
            result = consumerGroupService.reset(namespace, group, consumerGroupResetOffset, dryRun);
        }
        else if (dateTime != null) {
            result = consumerGroupService.toDateTime(namespace, group, consumerGroupResetOffset, dryRun);
        }
        else {
            result = consumerGroupResetOffset;
        }

        ConsumerGroupResetOffsetStatus status = (ConsumerGroupResetOffsetStatus)result.getStatus();
        if (status.success) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,green SUCCESS|@"));
            displayAsTable(status.getOffsetChanged());
        }

        return 0;
    }

    private void displayAsTable(Map<String, Long> offsetChanged) {
        CommandLine.Help.TextTable tt = CommandLine.Help.TextTable.forColumns(
                CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO),
                new CommandLine.Help.Column[]
                        {
                                new CommandLine.Help.Column(50, 2, CommandLine.Help.Column.Overflow.SPAN),
                                new CommandLine.Help.Column(30, 2, CommandLine.Help.Column.Overflow.SPAN)
                        });
        tt.addRowValues("PARTITION", "OFFSET");
        offsetChanged.forEach((name, offset) -> tt.addRowValues(name, offset.toString()));
        System.out.println(tt);
    }

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class ConsumerGroupResetOffsetStatus {
        private boolean success;
        private Map<String, Long> offsetChanged;
    }
}
