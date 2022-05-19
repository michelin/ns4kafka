package com.michelin.ns4kafka.cli;

import com.michelin.ns4kafka.cli.models.ObjectMeta;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.FormatService;
import com.michelin.ns4kafka.cli.services.LoginService;
import com.michelin.ns4kafka.cli.services.ResourceService;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(name = "reset-offsets", description = "Reset Consumer Group offsets")
public class ResetOffsetsSubcommand implements Callable<Integer> {
    /**
     * The login service
     */
    @Inject
    public LoginService loginService;

    /**
     * The resource service
     */
    @Inject
    public ResourceService resourceService;

    /**
     * The format service
     */
    @Inject
    public FormatService formatService;

    /**
     * The kafkactl configuration
     */
    @Inject
    public KafkactlConfig kafkactlConfig;

    /**
     * The Kafkactl command
     */
    @CommandLine.ParentCommand
    public KafkactlCommand kafkactlCommand;

    /**
     * The consumer group to reset
     */
    @Option(names = {"--group"}, required = true, description = "Consumer group name")
    public String group;

    /**
     * Is the command run with dry run mode
     */
    @Option(names = {"--dry-run"}, description = "Does not persist resources. Validate only")
    public boolean dryRun;

    /**
     * The topic to reset the consumer group
     */
    @ArgGroup(exclusive = true, multiplicity = "1")
    public TopicArgs topic;

    /**
     * The offset reset method
     */
    @ArgGroup(exclusive = true, multiplicity = "1")
    public ResetMethod method;

    public static class TopicArgs {
        /**
         * Reset offsets on a single topic or topic:partition
         */
        @Option(names = {"--topic"}, required = true, description = "Topic or Topic:Partition [ topic[:partition] ]")
        public String topic;

        /**
         * Reset offsets of all topics
         */
        @Option(names = {"--all-topics"}, required = true, description = "All topics")
        public boolean allTopics;
    }

    public static class ResetMethod {
        /**
         * Reset offsets to earliest
         */
        @Option(names = {"--to-earliest"}, description = "Set offset to its earliest value [ reprocess all ]", required = true)
        public boolean earliest;

        /**
         * Reset offsets to latest
         */
        @Option(names = {"--to-latest"}, description = "Set offset to its latest value [ skip all ]", required = true)
        public boolean latest;

        /**
         * Reset offsets to given datetime
         */
        @Option(names = {"--to-datetime"}, description = "Set offset to a specific ISO 8601 DateTime with Time zone [ yyyy-MM-dd'T'HH:mm:ss.SSSXXX ]", required = true)
        public OffsetDateTime datetime;

        /**
         * Reset offsets by shifting a given number of offsets
         */
        @Option(names = {"--shift-by"}, description = "Shift offset by a number [ negative to reprocess, positive to skip ]", required = true)
        public Integer shiftBy;

        /**
         * Reset offsets by given duration
         */
        @Option(names = {"--by-duration"}, description = "Shift offset by a duration format [ PnDTnHnMnS ]", required = true)
        public Duration duration;

        /**
         * Reset offsets to a given offset
         */
        @Option(names = {"--to-offset"}, description = "Set offset to a specific index", required = true)
        public Integer offset;
    }

    /**
     * The current command
     */
    @CommandLine.Spec
    public CommandLine.Model.CommandSpec commandSpec;

    /**
     * Run the reset offsets command
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

        Map<String, Object> consumerGroupResetOffsetSpec = new HashMap<>();

        consumerGroupResetOffsetSpec.put("topic", topic.allTopics ? "*" : topic.topic);

        if (method.earliest) {
            consumerGroupResetOffsetSpec.put("method", "TO_EARLIEST");
        } else if (method.latest) {
            consumerGroupResetOffsetSpec.put("method", "TO_LATEST");
        } else if (method.datetime != null) {
            consumerGroupResetOffsetSpec.put("method", "TO_DATETIME");
            consumerGroupResetOffsetSpec.put("options", method.datetime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        } else if (method.shiftBy != null) {
            consumerGroupResetOffsetSpec.put("method", "SHIFT_BY");
            consumerGroupResetOffsetSpec.put("options", method.shiftBy);
        } else if (method.duration != null) {
            consumerGroupResetOffsetSpec.put("method", "BY_DURATION");
            consumerGroupResetOffsetSpec.put("options", method.duration.toString());
        } else if (method.offset != null) {
            consumerGroupResetOffsetSpec.put("method", "TO_OFFSET");
            consumerGroupResetOffsetSpec.put("options", method.offset);
        }

        Resource consumerGroupResetOffset = Resource.builder()
                .apiVersion("v1")
                .kind("ConsumerGroupResetOffsets")
                .metadata(ObjectMeta.builder()
                        .namespace(namespace)
                        .name(group)
                        .build())
                .spec(consumerGroupResetOffsetSpec)
                .build();

        List<Resource> resources = resourceService.resetOffsets(namespace, group, consumerGroupResetOffset, dryRun);
        if (!resources.isEmpty()) {
            formatService.displayList("ConsumerGroupResetOffsetsResponse", resources, "table");
            return 0;
        }

        return 1;
    }
}
