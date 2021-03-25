package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.AccessControlEntryRepository;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.services.KafkaAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TopicControllerTest {

    @Mock
    NamespaceRepository namespaceRepository;
    @Mock
    TopicRepository topicRepository;
    @Mock
    AccessControlEntryRepository accessControlEntryRepository;
    @Mock
    ApplicationContext applicationContext;

    @InjectMocks
    TopicController topicController;

    @Test
    public void ListEmptyTopics() {
        when(topicRepository.findAllForNamespace("test"))
                .thenReturn(List.of());

        List<Topic> actual = topicController.list("test");
        Assertions.assertEquals(0, actual.size());
    }

    @Test
    public void ListMultipleTopics() {
        when(topicRepository.findAllForNamespace("test"))
                .thenReturn(List.of(
                        Topic.builder().metadata(ObjectMeta.builder().name("topic1").build()).build(),
                        Topic.builder().metadata(ObjectMeta.builder().name("topic2").build()).build()
                ));

        List<Topic> actual = topicController.list("test");

        Assertions.assertEquals(2, actual.size());
        Assertions.assertEquals("topic1", actual.get(0).getMetadata().getName());
        Assertions.assertEquals("topic2", actual.get(1).getMetadata().getName());
    }

    @Test
    public void GetEmptyTopic() {
        when(topicRepository.findByName("test", "topic.notfound"))
                .thenReturn(Optional.empty());

        Optional<Topic> actual = topicController.getTopic("test", "topic.notfound");

        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    public void GetTopic() {
        when(topicRepository.findByName("test", "topic.found"))
                .thenReturn(Optional.of(
                        Topic.builder().metadata(ObjectMeta.builder().name("topic.found").build()).build()
                ));

        Optional<Topic> actual = topicController.getTopic("test", "topic.found");

        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals("topic.found", actual.get().getMetadata().getName());
    }

    @Test
    public void DeleteTopic() throws InterruptedException, ExecutionException, TimeoutException {
        //Given
        when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().cluster("cluster1").build()));
        Optional<Topic> toDelete = Optional.of(Topic.builder().metadata(ObjectMeta.builder().name("topic.delete").build()).build());
        when(topicRepository.findByName("test", "topic.delete"))
                .thenReturn(toDelete);
        when(accessControlEntryRepository.findAllGrantedToNamespace("test"))
                .thenReturn(List.of(AccessControlEntry.builder()
                        .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                                .permission(AccessControlEntry.Permission.OWNER)
                                .resource("topic.delete")
                                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                                .grantedTo("test")
                                .build())
                        .build()));
        doNothing().when(topicRepository).delete(toDelete.get());
        KafkaAsyncExecutor kafkaAsyncExecutor = mock(KafkaAsyncExecutor.class);
        when(applicationContext.getBean(
                KafkaAsyncExecutor.class,
                Qualifiers.byName("cluster1")))
                .thenReturn(kafkaAsyncExecutor);
        doNothing().when(kafkaAsyncExecutor).deleteTopic(toDelete.get());

        //When
        HttpResponse<Void> actual = topicController.deleteTopic("test", "topic.delete");

        //Then
        Assertions.assertEquals(HttpStatus.NO_CONTENT, actual.getStatus());
    }

    @Test
    public void DeleteTopicUnauthorized() throws InterruptedException, ExecutionException, TimeoutException {
        //Given
        when(namespaceRepository.findByName("test"))
                .thenReturn(Optional.of(Namespace.builder().cluster("cluster1").build()));
        when(accessControlEntryRepository.findAllGrantedToNamespace("test"))
                .thenReturn(List.of()); //no ACL

        //When
        HttpResponse<Void> actual = topicController.deleteTopic("test", "topic.delete");

        //Then
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, actual.getStatus());

    }


    @Test
    public void CreateNewTopicFailValidationNoAPI() {
        /*Topic topic = Topic.builder()
                .metadata(ObjectMeta.builder().name("test.topic2").build())
                .spec(Topic.TopicSpec.builder()
                        .replicationFactor(1)
                        .partitions(3)
                        .build())
                .build();
        ResourceValidationException actual = Assertions.assertThrows(ResourceValidationException.class,
                () -> topicController.apply("test",topic));
        Assertions.assertEquals(1, actual.getValidationErrors().size());
        Assertions.assertLinesMatch(List.of(".*replication\\.factor.*"), actual.getValidationErrors());
         */
    }
}
