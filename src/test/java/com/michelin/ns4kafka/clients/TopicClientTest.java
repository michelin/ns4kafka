package com.michelin.ns4kafka.clients;

import com.michelin.ns4kafka.controllers.TopicController;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.ResourceValidationException;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.*;
import org.opentest4j.AssertionFailedError;
import scala.reflect.api.Names;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@MicronautTest()
@Property(name = "micronaut.security.enabled", value = "false")
public class TopicClientTest {
    /**
     * All these tests are API tests (using client)
     */

    @Inject
    @Client("/")
    RxHttpClient client;

    @Inject
    TopicController topicController;

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    @BeforeAll
    public void setupNamespacesAndACL(){
        Namespace ns = Namespace.builder()
                .topicValidator(TopicValidator.builder()
                        .validationConstraints(Map.of("replication.factor", ResourceValidator.Range.between(3,3)))
                        .build())
                .name("test")
                .cluster("fake")
                .build();
        namespaceRepository.createNamespace(ns);
        AccessControlEntry ace = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().name("test-ace1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resource("test.")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("test")
                        .build())
                .build();
        accessControlEntryRepository.create(ace);
    }

    @Test
    @Order(0)
    public void ListNoTopics(){
        List<Topic> actual = client.toBlocking().retrieve(HttpRequest.GET("/api/namespaces/test/topics"), Argument.listOf(Topic.class));
        Assertions.assertEquals(0,actual.size());
    }

    @Test
    @Order(10)
    public void CreateTopic(){
        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("test.topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(3)
                        .partitions(3)
                        .build())
                .build();
        Topic actual = client.toBlocking().retrieve(HttpRequest.POST("/api/namespaces/test/topics",topic), Topic.class);
        Assertions.assertEquals(Topic.TopicPhase.Pending, actual.getStatus().getPhase());
    }

    @Test
    @Order(20)
    public void GetTopic(){
        Optional<Topic> actual = client.toBlocking().retrieve(
                HttpRequest.GET("/api/namespaces/test/topics/test.topic1"),
                Argument.of(Optional.class,Argument.of(Topic.class)));


        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("test.topic1", actual.get().getMetadata().getName());
        Assertions.assertEquals(3,actual.get().getSpec().getReplicationFactor());
    }



    @Test
    @Order(30)
    public void UpdateTopicWithPartitionChange(){

    }

    @Test
    @Order(100)
    public void CreateNewTopicFailsValidation(){
        Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("test.topic1").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(1)
                        .partitions(3)
                        .build())
                .build();
        HttpClientResponseException actual = Assertions.assertThrows(HttpClientResponseException.class,
                () -> client.toBlocking().retrieve(HttpRequest.POST("/api/namespaces/test/topics",topic), Topic.class));
        Assertions.assertEquals("Message validation failed", actual.getMessage());
    }

}
