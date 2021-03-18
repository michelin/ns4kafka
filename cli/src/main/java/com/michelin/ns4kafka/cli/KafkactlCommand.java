package com.michelin.ns4kafka.cli;

import io.micronaut.configuration.picocli.PicocliRunner;
import io.micronaut.context.ApplicationContext;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "kafkactl", description = "...",
        mixinStandardHelpOptions = true)
public class KafkactlCommand implements Runnable {

    @Option(names = {"-v", "--verbose"}, description = "...")
    boolean verbose;

    public static void main(String[] args) throws Exception {
        PicocliRunner.run(KafkactlCommand.class, args);
    }

    public void run() {
        // business logic here
        System.out.println("Hi!");
        if (verbose) {
            System.out.println("Hi!");
        }
    }
}