package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.TagTopicInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.*;

import static com.michelin.ns4kafka.utils.config.ClusterConfig.CLUSTER_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TopicAsyncExecutorTest {

    private static final String CLUSTER_ID_TEST = "cluster_id_test";
    private static final String LOCAL_CLUSTER = "local";
    private static final String TOPIC_NAME = "topic";
    private static final String TAG1 = "TAG1";
    private static final String TAG2 = "TAG2";

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Test
    public void createTagsShouldAddTags() {
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig(LOCAL_CLUSTER);
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);
        TopicAsyncExecutorMock topicAsyncExecutor = new TopicAsyncExecutorMock(kafkaAsyncExecutorConfig);
        topicAsyncExecutor.setSchemaRegistryClient(schemaRegistryClient);

        when(schemaRegistryClient.addTags(anyString(), anyList())).thenReturn(Mono.just(new ArrayList<>()));

        List<Topic> ns4kafkaTopics = new ArrayList<>();
        Topic ns4kafkaTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .tags(List.of(TAG1))
                        .name(TOPIC_NAME).build()).build();
        ns4kafkaTopics.add(ns4kafkaTopic);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .tags(List.of(TAG2))
                        .name(TOPIC_NAME).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.createTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient, times(1)).addTags(anyString(), anyList());
    }

    @Test
    public void createTagsShouldNotAddTags() {
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig(LOCAL_CLUSTER);
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);
        TopicAsyncExecutorMock topicAsyncExecutor = new TopicAsyncExecutorMock(kafkaAsyncExecutorConfig);
        topicAsyncExecutor.setSchemaRegistryClient(schemaRegistryClient);

        List<Topic> ns4kafkaTopics = new ArrayList<>();
        Topic ns4kafkaTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .tags(List.of(TAG1))
                        .name(TOPIC_NAME).build()).build();
        ns4kafkaTopics.add(ns4kafkaTopic);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .tags(List.of(TAG1))
                        .name(TOPIC_NAME).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.createTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient, times(0)).addTags(anyString(), anyList());
    }

    @Test
    public void deleteTagsShouldDeleteTags() {
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig(LOCAL_CLUSTER);
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);
        TopicAsyncExecutorMock topicAsyncExecutor = new TopicAsyncExecutorMock(kafkaAsyncExecutorConfig);
        topicAsyncExecutor.setSchemaRegistryClient(schemaRegistryClient);

        when(schemaRegistryClient.deleteTag(anyString(),anyString(),anyString())).thenReturn(Mono.just(new HttpResponseMock()));

        List<Topic> ns4kafkaTopics = new ArrayList<>();
        Topic ns4kafkaTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .tags(List.of(TAG2))
                        .name(TOPIC_NAME).build()).build();
        ns4kafkaTopics.add(ns4kafkaTopic);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .tags(List.of(TAG1,TAG2))
                        .name(TOPIC_NAME).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.deleteTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient, times(1)).deleteTag(anyString(),anyString(),anyString());
    }

    @Test
    public void deleteTagsShouldNotDeleteTags() {
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig(LOCAL_CLUSTER);
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);
        TopicAsyncExecutorMock topicAsyncExecutor = new TopicAsyncExecutorMock(kafkaAsyncExecutorConfig);
        topicAsyncExecutor.setSchemaRegistryClient(schemaRegistryClient);

        List<Topic> ns4kafkaTopics = new ArrayList<>();
        Topic ns4kafkaTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .tags(List.of(TAG1))
                        .name(TOPIC_NAME).build()).build();
        ns4kafkaTopics.add(ns4kafkaTopic);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .tags(List.of(TAG1))
                        .name(TOPIC_NAME).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.deleteTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient, times(0)).deleteTag(anyString(),anyString(),anyString());
    }

    @Test
    public void completeWithTagsShouldComplete() {
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig(LOCAL_CLUSTER, KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD);
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);
        TopicAsyncExecutorMock topicAsyncExecutor = new TopicAsyncExecutorMock(kafkaAsyncExecutorConfig);
        topicAsyncExecutor.setSchemaRegistryClient(schemaRegistryClient);

        TagTopicInfo tagTopicInfo = TagTopicInfo.builder().typeName(TAG1).build();

        when(schemaRegistryClient.getTopicWithTags(anyString(),anyString())).thenReturn(Mono.just(List.of(tagTopicInfo)));

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.completeWithTags(brokerTopics);

        assertEquals(TAG1,brokerTopics.get(TOPIC_NAME).getMetadata().getTags().get(0));
    }

    @Test
    public void completeWithTagsShouldNotComplete() {
        KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig = new KafkaAsyncExecutorConfig(LOCAL_CLUSTER, KafkaAsyncExecutorConfig.KafkaProvider.SELF_MANAGED);
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);
        TopicAsyncExecutorMock topicAsyncExecutor = new TopicAsyncExecutorMock(kafkaAsyncExecutorConfig);
        topicAsyncExecutor.setSchemaRegistryClient(schemaRegistryClient);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.completeWithTags(brokerTopics);

        assertNull(brokerTopics.get(TOPIC_NAME).getMetadata().getTags());
    }
}
