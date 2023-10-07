package com.michelin.ns4kafka.services.executors;

import static com.michelin.ns4kafka.services.executors.TopicAsyncExecutor.CLUSTER_ID;
import static com.michelin.ns4kafka.services.executors.TopicAsyncExecutor.TOPIC_ENTITY_TYPE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class TopicAsyncExecutorTest {
    private static final String CLUSTER_ID_TEST = "cluster_id_test";
    private static final String LOCAL_CLUSTER = "local";
    private static final String TOPIC_NAME = "topic";
    private static final String TAG1 = "TAG1";
    private static final String TAG2 = "TAG2";
    private static final String TAG3 = "TAG3";

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @InjectMocks
    TopicAsyncExecutor topicAsyncExecutor;

    @Test
    void shouldDeleteTagsAndNotCreateIfEmpty() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(schemaRegistryClient.deleteTag(anyString(),
            anyString(), anyString()))
            .thenReturn(Mono.empty())
            .thenReturn(Mono.error(new Exception("error")));
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(managedClusterProperties.getConfig()).thenReturn(properties);

        List<Topic> ns4kafkaTopics = List.of(
            Topic.builder()
                .metadata(ObjectMeta.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .tags(List.of(TAG1))
                    .build())
                .build());

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(ObjectMeta.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .tags(List.of(TAG1, TAG2, TAG3))
                    .build())
                .build());

        topicAsyncExecutor.alterTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient).deleteTag(LOCAL_CLUSTER, CLUSTER_ID_TEST + ":" + TOPIC_NAME, TAG2);
        verify(schemaRegistryClient).deleteTag(LOCAL_CLUSTER, CLUSTER_ID_TEST + ":" + TOPIC_NAME, TAG3);
    }

    @Test
    void shouldCreateTags() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(schemaRegistryClient.addTags(anyString(), anyList()))
            .thenReturn(Mono.empty())
            .thenReturn(Mono.error(new Exception("error")));
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(managedClusterProperties.getConfig()).thenReturn(properties);

        List<Topic> ns4kafkaTopics = List.of(
            Topic.builder()
                .metadata(ObjectMeta.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .tags(List.of(TAG1))
                    .build())
                .build());

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(ObjectMeta.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .build())
                .build());

        topicAsyncExecutor.alterTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient).addTags(eq(LOCAL_CLUSTER), argThat(tags ->
            tags.get(0).entityName().equals(CLUSTER_ID_TEST + ":" + TOPIC_NAME)
                && tags.get(0).typeName().equals(TAG1)
                && tags.get(0).entityType().equals(TOPIC_ENTITY_TYPE)));
    }

    @Test
    void shouldBeConfluentCloud() {
        when(managedClusterProperties.getProvider()).thenReturn(ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        assertTrue(topicAsyncExecutor.isConfluentCloud());
    }
}
