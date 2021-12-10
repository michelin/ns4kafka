package com.michelin.ns4kafka.cli;

import io.micronaut.configuration.picocli.PicocliRunner;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;
import java.util.concurrent.Callable;

@Command(name = "kafkactl",
        subcommands =
                {
                        ApplySubcommand.class,
                        GetSubcommand.class,
                        DeleteSubcommand.class,
                        ApiResourcesSubcommand.class,
                        DiffSubcommand.class,
                        ResetOffsetsSubcommand.class,
                        DeleteRecordsSubcommand.class,
                        ImportSubcommand.class,
                        ConnectorsSubcommand.class,
                        SchemaSubcommand.class
                },
        versionProvider = KafkactlCommand.ConfigVersionProvider.class,
        mixinStandardHelpOptions = true)
public class KafkactlCommand implements Callable<Integer> {

    public static boolean VERBOSE = false;
    @Inject
    public ConfigVersionProvider versionProvider;

    @Option(names = {"-v", "--verbose"}, description = "...", scope = CommandLine.ScopeType.INHERIT)
    public void setVerbose(final boolean verbose) {
        VERBOSE = verbose;
    }

    @Option(names = {"-n", "--namespace"}, description = "Override namespace defined in config or yaml resource", scope = CommandLine.ScopeType.INHERIT)
    public Optional<String> optionalNamespace;


    public static void main(String[] args) throws Exception {
        // There are 3 ways to configure kafkactl :
        // 1. Setup config file in $HOME/.kafkactl/config.yml
        // 2. Setup config file anywhere and set KAFKACTL_CONFIG=/path/to/config.yml
        // 3. No file but environment variables instead
        boolean hasConfig = System.getenv().keySet().stream().anyMatch(s -> s.startsWith("KAFKACTL_"));
        if (!hasConfig) {
            System.setProperty("micronaut.config.files", System.getProperty("user.home") + "/.kafkactl/config.yml");
        }
        if (System.getenv("KAFKACTL_CONFIG") != null) {
            System.setProperty("micronaut.config.files", System.getenv("KAFKACTL_CONFIG"));
        }

        int exitCode = PicocliRunner.execute(KafkactlCommand.class, args);
        System.exit(exitCode);
    }

    public Integer call() throws Exception {
        CommandLine cmd = new CommandLine(new KafkactlCommand());
        // Display help
        System.out.println(versionProvider.getVersion()[0]);
        cmd.usage(System.out);

        return 0;

    }

    @Singleton
    public static class ConfigVersionProvider implements CommandLine.IVersionProvider {

        @Inject
        public KafkactlConfig kafkactlConfig;
        @Override
        public String[] getVersion() {
            return new String[]{
                    "v" + kafkactlConfig.getVersion()
            };
        }
    }

}
