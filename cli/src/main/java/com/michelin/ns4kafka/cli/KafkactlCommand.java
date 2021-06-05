package com.michelin.ns4kafka.cli;

import io.micronaut.configuration.picocli.PicocliRunner;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

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
                        TopicSubcommand.class
                },
                versionProvider = KafkactlCommand.ManifestVersionProvider.class,
                mixinStandardHelpOptions=true)
public class KafkactlCommand implements Callable<Integer> {

    public static boolean VERBOSE = false;

    @Option(names = {"-v", "--verbose"}, description = "...", scope = CommandLine.ScopeType.INHERIT)
    public void setVerbose(final boolean verbose) {
        VERBOSE = verbose;
    }

    @Option(names = {"-n", "--namespace"}, description = "Override namespace defined in config or yaml resource", scope = CommandLine.ScopeType.INHERIT)
    public Optional<String> optionalNamespace;


    public static void main(String[] args) throws Exception {
        int exitCode = PicocliRunner.execute(KafkactlCommand.class, args);
        System.exit(exitCode);
    }

    public Integer call() throws Exception {
        CommandLine cmd = new CommandLine(new KafkactlCommand());
        // Display help
        cmd.printVersionHelp(System.out);
        cmd.usage(System.out);

        return 0;

    }

    public static class ManifestVersionProvider implements CommandLine.IVersionProvider {

        @Override
        public String[] getVersion() {
            return new String[] {
                    "v" + getClass().getPackage().getImplementationVersion()
            };
        }
    }

}
