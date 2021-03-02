package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Topic;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.Optional;

@MicronautTest()
@Property(name = "kafka.embedded.enabled", value = "false")
@Property(name = "ns4kafka.store.kafka.enabled", value="false")
@Property(name = "ns4kafka.store.mock.enabled", value="true")
@Property(name = "micronaut.security.enabled", value = "false")
public class TopicControllerTest {

    @Inject
    @Client("/")
    RxHttpClient client;

    @Test
    public void GetTopic(){
        Optional<Topic> topic = client.toBlocking().retrieve(
                HttpRequest.GET("/api/namespaces/test/topics/toto"),
                Argument.of(Optional.class,Argument.of(Topic.class)));


        Assertions.assertTrue(topic.isPresent());
        Assertions.assertEquals("toto", topic.get().getMetadata().getName());
    }
    @Test
    public void CreateTopic(){

    }

    @Test
    public void UpdateTopicWithPartitionChange(){

    }

}
