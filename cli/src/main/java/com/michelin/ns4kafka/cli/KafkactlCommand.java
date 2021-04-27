package com.michelin.ns4kafka.cli;

import io.micronaut.configuration.picocli.PicocliRunner;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import javax.inject.Inject;
import java.util.concurrent.Callable;

@Command(name = "kafkactl", subcommands = {LoginSubcommand.class, ApplySubcommand.class, ListSubcommand.class, GetSubcommand.class, DeleteSubcommand.class} , description = "...",
        mixinStandardHelpOptions = false)
public class KafkactlCommand implements Callable<Integer> {


    @Option(names = {"-v", "--verbose"}, description = "...")
    boolean verbose;

    @Inject


    public static void main(String[] args) throws Exception {
        int exitCode = PicocliRunner.execute(KafkactlCommand.class, args);
        System.exit(exitCode);
    }

    public Integer call() throws Exception {
        // Check API status

        // Check login status

        // Check namespace is set

        // Display help
        CommandLine.usage(new KafkactlCommand(), System.out);

        return 0;

    }

}
