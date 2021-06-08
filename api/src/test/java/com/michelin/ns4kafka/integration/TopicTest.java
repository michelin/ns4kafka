package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.controllers.ResourceValidationException;
import com.michelin.ns4kafka.controllers.TopicController;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.repositories.kafka.KafkaTopicRepository;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@MicronautTest
public class TopicTest {

    @Inject
    TopicAsyncExecutor topicAsyncExecutor;

    @Inject
    TopicRepository topicRepository;
    
    @Inject
    NamespaceRepository namespaceRepository;
    
    @Inject
    AccessControlEntryRepository accessControlEntryRepository;

    @Inject
    TopicController topicController;

    @Test
    void login() throws InterruptedException, ExecutionException, TimeoutException {

        Namespace namespace = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("df4mgbl0")
                        .name("test")
                        .build()
                )
                .spec(
                        Namespace.NamespaceSpec.builder()
                                .topicValidator(TopicValidator.builder()
                                        .validationConstraints(
                                                Map.of("partitions", ResourceValidator.Range.between(3, 6),
                                                        "replication.factor", ResourceValidator.Range.between(1, 1)
                                                )
                                        ).build())
                                .build()
                )
                .build();

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder()
                        .namespace("test")
                        .name("access-test-topics")
                .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .grantedTo("test")
                        .resource("topic")
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();
        
        Topic topic_1 = Topic.builder()
                .metadata(
                        ObjectMeta.builder()
                                .name("topic.test.collide")
                                .cluster("df4mgbl0")
                                .namespace("test")
                                .build()
                )
                .spec(
                        Topic.TopicSpec.builder()
                                .partitions(3)
                                .replicationFactor(1)
                                .build()
                )
                .build();

        Topic topic_2 = Topic.builder()
                .metadata(
                        ObjectMeta.builder()
                                .name("topic_test_collide")
                                .cluster("df4mgbl0")
                                .namespace("test")
                                .build()
                )
                .spec(
                        Topic.TopicSpec.builder()
                                .partitions(3)
                                .replicationFactor(1)
                                .build()
                )
                .build();
        
        // create namespace
        namespaceRepository.createNamespace(namespace);
        // create access control entry
        accessControlEntryRepository.create(accessControlEntry);
        // create and synchronize topic
        topicRepository.create(topic_1);
        topicAsyncExecutor.synchronizeTopics();

        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class, () -> topicController.apply("test", topic_2, false));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertEquals("Topic " 
                + topic_1.getMetadata().getName() 
                + " collides with existing topics: " 
                + topic_2.getMetadata().getName(), actual.getValidationErrors().get(0));
        

    }
}
