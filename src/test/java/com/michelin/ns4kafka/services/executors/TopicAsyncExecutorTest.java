package com.michelin.ns4kafka.services.executors;

import static com.michelin.ns4kafka.services.executors.TopicAsyncExecutor.CLUSTER_ID;
import static com.michelin.ns4kafka.services.executors.TopicAsyncExecutor.TOPIC_ENTITY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.TagEntities;
import com.michelin.ns4kafka.services.clients.schema.entities.TagEntity;
import io.micronaut.http.HttpResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
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

    @Mock
    Admin adminClient;

    @Mock
    DeleteTopicsResult deleteTopicsResult;

    @Mock
    KafkaFuture<Void> kafkaFuture;

    @InjectMocks
    TopicAsyncExecutor topicAsyncExecutor;

    @Test
    void shouldDeleteTagsAndNotCreateIfEmpty() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(schemaRegistryClient.dissociateTag(anyString(),
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

        verify(schemaRegistryClient).dissociateTag(LOCAL_CLUSTER, CLUSTER_ID_TEST + ":" + TOPIC_NAME, TAG2);
        verify(schemaRegistryClient).dissociateTag(LOCAL_CLUSTER, CLUSTER_ID_TEST + ":" + TOPIC_NAME, TAG3);
    }

    @Test
    void shouldCreateTags() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(schemaRegistryClient.associateTags(anyString(), anyList()))
                .thenReturn(Mono.just(List.of()));
        when(schemaRegistryClient.createTags(anyList(), anyString()))
                .thenReturn(Mono.just(List.of()));
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

        verify(schemaRegistryClient).associateTags(eq(LOCAL_CLUSTER), argThat(tags ->
            tags.get(0).entityName().equals(CLUSTER_ID_TEST + ":" + TOPIC_NAME)
                && tags.get(0).typeName().equals(TAG1)
                && tags.get(0).entityType().equals(TOPIC_ENTITY_TYPE)));
    }

    @Test
    void shouldNotAssociateTagsWhenCreationFails() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(schemaRegistryClient.createTags(anyList(), anyString()))
                .thenReturn(Mono.error(new IOException()));
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

        verify(schemaRegistryClient, never()).associateTags(eq(LOCAL_CLUSTER), argThat(tags ->
                tags.get(0).entityName().equals(CLUSTER_ID_TEST + ":" + TOPIC_NAME)
                        && tags.get(0).typeName().equals(TAG1)
                        && tags.get(0).entityType().equals(TOPIC_ENTITY_TYPE)));
    }

    @Test
    void shouldBeConfluentCloud() {
        when(managedClusterProperties.getProvider()).thenReturn(ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        assertTrue(topicAsyncExecutor.isConfluentCloud());
    }

    @Test
    void shouldDeleteTopicNoTags() throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.getProvider()).thenReturn(ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .build())
            .build();

        topicAsyncExecutor.deleteTopic(topic);

        verify(schemaRegistryClient, never()).dissociateTag(any(), any(), any());
    }

    @Test
    void shouldDeleteTopicSelfManagedCluster() throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.getProvider()).thenReturn(ManagedClusterProperties.KafkaProvider.SELF_MANAGED);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .build())
            .build();

        topicAsyncExecutor.deleteTopic(topic);

        verify(schemaRegistryClient, never()).dissociateTag(any(), any(), any());
    }

    @Test
    void shouldDeleteTopicAndTags() throws ExecutionException, InterruptedException, TimeoutException {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(managedClusterProperties.getProvider()).thenReturn(ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(managedClusterProperties.getConfig()).thenReturn(properties);
        when(schemaRegistryClient.dissociateTag(anyString(),
            anyString(), anyString()))
            .thenReturn(Mono.just(HttpResponse.ok()))
            .thenReturn(Mono.error(new Exception("error")));

        Topic topic = Topic.builder()
            .metadata(ObjectMeta.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of(TAG1))
                .build())
            .build();

        topicAsyncExecutor.deleteTopic(topic);

        verify(schemaRegistryClient).dissociateTag(LOCAL_CLUSTER, CLUSTER_ID_TEST + ":" + TOPIC_NAME, TAG1);
    }

    @Test
    void shouldNotEnrichWithTagsWhenNotConfluentCloud() {
        when(managedClusterProperties.getProvider()).thenReturn(ManagedClusterProperties.KafkaProvider.SELF_MANAGED);

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(ObjectMeta.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .build())
                .build());

        topicAsyncExecutor.enrichWithTags(brokerTopics);

        assertTrue(brokerTopics.get(TOPIC_NAME).getSpec().getTags().isEmpty());
    }

    @Test
    void shouldEnrichWithTagsWhenConfluentCloud() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(managedClusterProperties.getProvider()).thenReturn(ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);

        TagEntity tagEntity = TagEntity.builder()
                .classificationNames(List.of("typeName"))
                .displayText(TOPIC_NAME).build();
        List<TagEntity> tagEntityList = List.of(tagEntity);
        TagEntities tagEntities = TagEntities.builder().entities(tagEntityList).build();

        when(schemaRegistryClient.getTopicWithTags(LOCAL_CLUSTER))
            .thenReturn(Mono.just(tagEntities));

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(ObjectMeta.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .build())
                .build());

        topicAsyncExecutor.enrichWithTags(brokerTopics);

        assertEquals("typeName", brokerTopics.get(TOPIC_NAME).getSpec().getTags().get(0));
    }

    @Test
    void shouldEnrichWithTagsWhenConfluentCloudAndResponseIsNull() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(managedClusterProperties.getProvider()).thenReturn(ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);

        when(schemaRegistryClient.getTopicWithTags(LOCAL_CLUSTER))
            .thenReturn(Mono.empty());

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(ObjectMeta.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .build())
                .build());

        topicAsyncExecutor.enrichWithTags(brokerTopics);

        assertTrue(brokerTopics.get(TOPIC_NAME).getSpec().getTags().isEmpty());
    }
}
