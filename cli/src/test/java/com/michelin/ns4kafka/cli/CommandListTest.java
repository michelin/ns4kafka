package com.michelin.ns4kafka.cli;

import io.micronaut.configuration.picocli.PicocliRunner;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.Environment;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommandListTest {

    @Test
    public void testWithNoNamespaceAndNoConfig() throws Exception {
        //TODO Mock getJWT
        //try (ApplicationContext ctx = ApplicationContext.run(Environment.CLI, Environment.TEST)) {
        //    String[] args = new String[] { "-k", "topic" };
        //
        //    int exitCode = PicocliRunner.execute(CommandList.class, args);

        //    // kafkactl
        //    assertEquals(2, exitCode);
        //}
    }
}
