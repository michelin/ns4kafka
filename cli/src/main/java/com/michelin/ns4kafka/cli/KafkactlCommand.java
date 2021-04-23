package com.michelin.ns4kafka.cli;

import java.util.concurrent.Callable;

import io.micronaut.configuration.picocli.PicocliRunner;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "kafkactl", subcommands = {LoginSubcommand.class, ApplySubcommand.class, ListSubcommand.class, GetSubcommand.class, DeleteSubcommand.class} , description = "...",
        mixinStandardHelpOptions = true)
public class KafkactlCommand implements Callable<Integer> {


    @Option(names = {"-v", "--verbose"}, description = "...")
    boolean verbose;


    public static void main(String[] args) throws Exception {
        int exitCode = PicocliRunner.execute(KafkactlCommand.class, args);
        System.exit(exitCode);
    }

    public Integer call() throws Exception {
        // business logic here
        System.out.println("Hi!");
        if (verbose) {
            System.out.println("Hi!");
        }
        return 0;

    }

}
