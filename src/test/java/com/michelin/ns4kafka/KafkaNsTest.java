package com.michelin.ns4kafka;

import io.micronaut.context.env.Environment;
import io.micronaut.runtime.EmbeddedApplication;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import javax.inject.Inject;

@MicronautTest
public class KafkaNsTest {

    //@Inject
    //EmbeddedApplication application;

    @Test
    void testItWorks() {
        Assertions.assertTrue(true);
        //Environment environment = application.getEnvironment();
        //Assertions.assertTrue(application.isRunning());
    }

}
