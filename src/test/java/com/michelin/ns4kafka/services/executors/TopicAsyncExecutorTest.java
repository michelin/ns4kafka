package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.TagSpecs;
import com.michelin.ns4kafka.services.clients.schema.entities.TagTopicInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.shaded.org.hamcrest.Matchers;
import reactor.core.publisher.Mono;

import java.util.*;

import static com.michelin.ns4kafka.utils.config.ClusterConfig.CLUSTER_ID;
import static com.michelin.ns4kafka.utils.tags.TagsUtils.TOPIC_ENTITY_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TopicAsyncExecutorTest {

    private static final String CLUSTER_ID_TEST = "cluster_id_test";
    private static final String LOCAL_CLUSTER = "local";
    private static final String TOPIC_NAME = "topic";
    private static final String TAG1 = "TAG1";
    private static final String TAG2 = "TAG2";

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Mock
    KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;

    @InjectMocks
    TopicAsyncExecutor topicAsyncExecutor;

    @Test
    void createTagsShouldAddTags() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);

        when(schemaRegistryClient.addTags(anyString(), anyList())).thenReturn(Mono.just(new ArrayList<>()));
        when(kafkaAsyncExecutorConfig.getConfig()).thenReturn(properties);
        when(kafkaAsyncExecutorConfig.getName()).thenReturn(LOCAL_CLUSTER);

        List<Topic> ns4kafkaTopics = new ArrayList<>();
        Topic ns4kafkaTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of(TAG1)).build()).build();
        ns4kafkaTopics.add(ns4kafkaTopic);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of(TAG2)).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.createTags(ns4kafkaTopics, brokerTopics);

        List<TagSpecs> tagSpecsList = new ArrayList<>();
        TagSpecs tagSpecs = TagSpecs.builder().typeName(TAG1).entityName(CLUSTER_ID_TEST+":"+TOPIC_NAME).entityType(TOPIC_ENTITY_TYPE).build();
        tagSpecsList.add(tagSpecs);
        verify(schemaRegistryClient, times(1)).addTags(eq(LOCAL_CLUSTER), argThat(new TagSpecsArgumentMatcher(tagSpecsList)));
    }

    @Test
    void createTagsShouldNotAddTags() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);

        List<Topic> ns4kafkaTopics = new ArrayList<>();
        Topic ns4kafkaTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of(TAG1)).build()).build();
        ns4kafkaTopics.add(ns4kafkaTopic);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of(TAG1)).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.createTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient, times(0)).addTags(anyString(), anyList());
    }

    @Test
    void deleteTagsShouldDeleteTags() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);

        when(schemaRegistryClient.deleteTag(anyString(),anyString(),anyString())).thenReturn(Mono.just(new HttpResponseMock()));
        when(kafkaAsyncExecutorConfig.getConfig()).thenReturn(properties);
        when(kafkaAsyncExecutorConfig.getName()).thenReturn(LOCAL_CLUSTER);

        List<Topic> ns4kafkaTopics = new ArrayList<>();
        Topic ns4kafkaTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of(TAG2)).build()).build();
        ns4kafkaTopics.add(ns4kafkaTopic);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of(TAG1,TAG2)).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.deleteTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient, times(1)).deleteTag(eq(LOCAL_CLUSTER),eq(CLUSTER_ID_TEST+":"+TOPIC_NAME),eq(TAG1));
    }

    @Test
    void deleteTagsShouldNotDeleteTags() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);

        List<Topic> ns4kafkaTopics = new ArrayList<>();
        Topic ns4kafkaTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of(TAG1)).build()).build();
        ns4kafkaTopics.add(ns4kafkaTopic);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder()
                        .tags(List.of(TAG1)).build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.deleteTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient, times(0)).deleteTag(anyString(),anyString(),anyString());
    }

    @Test
    void completeWithTagsShouldComplete() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);

        TagTopicInfo tagTopicInfo = TagTopicInfo.builder().typeName(TAG1).build();

        when(schemaRegistryClient.getTopicWithTags(anyString(),anyString())).thenReturn(Mono.just(List.of(tagTopicInfo)));
        when(kafkaAsyncExecutorConfig.getConfig()).thenReturn(properties);
        when(kafkaAsyncExecutorConfig.getName()).thenReturn(LOCAL_CLUSTER);
        when(kafkaAsyncExecutorConfig.getProvider()).thenReturn(KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder().build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.completeWithTags(brokerTopics);

        assertEquals(TAG1,brokerTopics.get(TOPIC_NAME).getSpec().getTags().get(0));
    }

    @Test
    void completeWithTagsShouldNotComplete() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID,CLUSTER_ID_TEST);
        kafkaAsyncExecutorConfig.setConfig(properties);

        when(kafkaAsyncExecutorConfig.getProvider()).thenReturn(KafkaAsyncExecutorConfig.KafkaProvider.SELF_MANAGED);

        Map<String, Topic> brokerTopics = new HashMap<>();
        Topic brokerTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name(TOPIC_NAME).build())
                .spec(Topic.TopicSpec.builder().build()).build();
        brokerTopics.put(TOPIC_NAME, brokerTopic);

        topicAsyncExecutor.completeWithTags(brokerTopics);

        assertNull(brokerTopics.get(TOPIC_NAME).getSpec().getTags());
    }
}
