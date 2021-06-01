package com.michelin.ns4kafka.cli;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ConsumerGroupService;
import com.michelin.ns4kafka.cli.services.LoginService;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
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
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    public boolean dryRun;

    @ArgGroup(exclusive = true, multiplicity = "1")
    public ResetMethod method;

    public static class ResetMethod {
        @Option(names = {"--to-earliest"}, description = "Set offset to its earliest value", required = true)
        public boolean earliest;
        @Option(names = {"--to-latest"}, description = "Set offset to its latest value", required = true)
        public boolean latest;
        @Option(names = {"--to-datetime"}, description = "Set offset to a specific ISO 8601 DateTime with Time zone [ yyyy-MM-dd'T'HH:mm:ss.SSSXXX ] formats", required = true)
        public String datetime;
        @Option(names = {"--shift-by"}, description = "Shift offset by a number", required = true)
        public Integer shiftBy;
        @Option(names = {"--by-duration"}, description = "Shift offset by a duration format [ PnDTnHnMnS ]", required = true)
        public Duration duration;
    }

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

        Map<String, Object> consumerGroupResetOffsetSpec = new HashMap<>();
        consumerGroupResetOffsetSpec.put("topic", topic);
        if (method.earliest) {
            consumerGroupResetOffsetSpec.put("method", "TO_EARLIEST");
        }
        else if (method.latest) {
            consumerGroupResetOffsetSpec.put("method", "TO_LATEST");
        }
        else if (method.datetime != null) {
            consumerGroupResetOffsetSpec.put("method", "TO_DATETIME");
            consumerGroupResetOffsetSpec.put("options", method.datetime);
        }
        else if (method.shiftBy != null) {
            consumerGroupResetOffsetSpec.put("method", "SHIFT_BY");
            consumerGroupResetOffsetSpec.put("options", method.shiftBy.intValue());
        }
        else if (method.duration != null) {
            consumerGroupResetOffsetSpec.put("method", "DURATION");
            consumerGroupResetOffsetSpec.put("options", method.duration.toString());
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

        try {
            result = consumerGroupService.reset(namespace, group, consumerGroupResetOffset, dryRun);
        } catch (HttpClientResponseException e) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,red ERROR: |@" + e.getMessage()));
            return 1;
        }

        ConsumerGroupResetOffsetStatus status = (ConsumerGroupResetOffsetStatus)result.getStatus();
        if (status.success) {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,green SUCCESS|@"));
            displayAsTable(status.getOffsetChanged());
        } else {
            System.out.println(CommandLine.Help.Ansi.AUTO.string("@|bold,red ERROR: |@" + status.getErrorMessage()));
            return 1;
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
        private String errorMessage;
        private Map<String, Long> offsetChanged;

    }
}
