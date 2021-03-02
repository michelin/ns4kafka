package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaNamespaceRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaStoreException;
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;

@MicronautTest()
@Property(name = "micronaut.security.enabled", value = "false")
public class TopicControllerTest {

    @Inject
    @Client("/")
    RxHttpClient client;

    @Inject
    NamespaceRepository namespaceRepository;

    @Test
    public void Test(){
        when(namespaceRepository.findByName("test"))
                .then(invocation -> Optional.of(Namespace.builder().name("test").build()));



        Optional<Namespace> namespaceOptional = client.toBlocking()
                .retrieve(HttpRequest.GET("/api/namespaces/test/"),
                        Argument.of(Optional.of(Namespace.class).getClass()));


        Assertions.assertTrue(namespaceOptional.isPresent());
        /*namespaceRepository.createNamespace(
                Namespace.builder()
                        .cluster("cloud")
                        .defaulKafkatUser("test_user")
                        .diskQuota(99)
                        .name("ns01")
                        .build()
        );
        Assertions.assertEquals(1, namespaceRepository.findAllForCluster("cloud").size());
        */
    }
    @MockBean(KafkaNamespaceRepository.class)
    NamespaceRepository namespaceRepository(){
        return mock(KafkaNamespaceRepository.class);
    }
}
