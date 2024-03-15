package com.michelin.ns4kafka.services.executors;

import static com.michelin.ns4kafka.services.executors.TopicAsyncExecutor.CLUSTER_ID;
import static com.michelin.ns4kafka.services.executors.TopicAsyncExecutor.TOPIC_ENTITY_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.services.clients.schema.SchemaRegistryClient;
import com.michelin.ns4kafka.services.clients.schema.entities.TagEntities;
import com.michelin.ns4kafka.services.clients.schema.entities.TagEntity;
import com.michelin.ns4kafka.services.clients.schema.entities.TagInfo;
import com.michelin.ns4kafka.services.clients.schema.entities.TagTopicInfo;
import com.michelin.ns4kafka.services.clients.schema.entities.TopicDescriptionUpdateResponse;
import com.michelin.ns4kafka.services.clients.schema.entities.TopicListResponse;
import com.michelin.ns4kafka.services.clients.schema.entities.TopicListResponseEntity;
import com.michelin.ns4kafka.services.clients.schema.entities.TopicListResponseEntityAttributes;
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
    private static final String TOPIC_NAME2 = "topic2";
    private static final String TOPIC_NAME3 = "topic3";
    private static final String TAG1 = "TAG1";
    private static final String TAG2 = "TAG2";
    private static final String TAG3 = "TAG3";
    private static final String DESCRIPTION1 = "My topic description";
    private static final String DESCRIPTION2 = "Another description";

    @Mock
    SchemaRegistryClient schemaRegistryClient;

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @Mock
    TopicRepository topicRepository;

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
                .metadata(Metadata.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .tags(List.of(TAG1))
                    .build())
                .build());

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(Metadata.builder()
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
    void shouldDeleteTagsAndCreateAssociateIfNotEmpty() {
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
                .metadata(Metadata.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .tags(List.of(TAG1, TAG3))
                    .build())
                .build());

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(Metadata.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .tags(List.of(TAG1, TAG2))
                    .build())
                .build());

        List<TagInfo> tagsToCreate = List.of(TagInfo.builder().name(TAG3).build());
        List<TagTopicInfo> tagsToAssociate = List.of(TagTopicInfo.builder()
            .entityName(managedClusterProperties
                .getConfig()
                .getProperty(CLUSTER_ID) + ":" + TOPIC_NAME)
            .typeName(TAG3)
            .entityType(TOPIC_ENTITY_TYPE)
            .build());

        when(schemaRegistryClient.createTags(any(), any())).thenReturn(Mono.just(tagsToCreate));
        when(schemaRegistryClient.associateTags(any(), any())).thenReturn(Mono.just(tagsToAssociate));

        topicAsyncExecutor.alterTags(ns4kafkaTopics, brokerTopics);

        verify(schemaRegistryClient).dissociateTag(LOCAL_CLUSTER, CLUSTER_ID_TEST + ":" + TOPIC_NAME, TAG2);
        verify(schemaRegistryClient).createTags(LOCAL_CLUSTER, tagsToCreate);
        verify(schemaRegistryClient).associateTags(LOCAL_CLUSTER, tagsToAssociate);
    }

    @Test
    void shouldNotCreateTagsWhenFail() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(managedClusterProperties.getConfig()).thenReturn(properties);

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .generation(0)
                .build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of(TAG1))
                .build())
            .build();

        TagTopicInfo tagTopicInfo = TagTopicInfo.builder()
            .entityName(managedClusterProperties
                .getConfig()
                .getProperty(CLUSTER_ID) + ":" + topic.getMetadata().getName())
            .typeName(TAG1)
            .entityType(TOPIC_ENTITY_TYPE)
            .build();

        Map<Topic, List<TagTopicInfo>> topicTagsMapping = Map.of(topic, List.of(tagTopicInfo));

        when(schemaRegistryClient.createTags(any(), any())).thenReturn(Mono.error(new IOException()));

        topicAsyncExecutor.createAndAssociateTags(topicTagsMapping);

        assertEquals(0, topic.getMetadata().getGeneration());
    }

    @Test
    void shouldNotAssociateTagsWhenFail() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(managedClusterProperties.getConfig()).thenReturn(properties);

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .generation(0)
                .build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of(TAG1))
                .build())
            .build();

        TagTopicInfo tagTopicInfo = TagTopicInfo.builder()
                .entityName(managedClusterProperties
                        .getConfig()
                        .getProperty(CLUSTER_ID) + ":" + topic.getMetadata().getName())
                .typeName(TAG1)
                .entityType(TOPIC_ENTITY_TYPE)
                .build();

        Map<Topic, List<TagTopicInfo>> topicTagsMapping = Map.of(topic, List.of(tagTopicInfo));
        List<TagInfo> response = List.of(TagInfo.builder().name(TAG1).build());

        when(schemaRegistryClient.createTags(any(), any())).thenReturn(Mono.just(response));
        when(schemaRegistryClient.associateTags(any(), any())).thenReturn(Mono.error(new IOException()));

        topicAsyncExecutor.createAndAssociateTags(topicTagsMapping);

        verify(topicRepository).create(topic);
    }

    @Test
    void shouldCreateAndAssociateTags() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        when(managedClusterProperties.getConfig()).thenReturn(properties);

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .generation(0)
                .build())
            .spec(Topic.TopicSpec.builder()
                .tags(List.of(TAG1))
                .build())
            .build();

        TagTopicInfo tagTopicInfo = TagTopicInfo.builder()
            .entityName(managedClusterProperties
                .getConfig()
                .getProperty(CLUSTER_ID) + ":" + topic.getMetadata().getName())
            .typeName(TAG1)
            .entityType(TOPIC_ENTITY_TYPE)
            .build();

        Map<Topic, List<TagTopicInfo>> topicTagsMapping = Map.of(topic, List.of(tagTopicInfo));

        List<TagInfo> response = List.of(TagInfo.builder().name(TAG1).build());
        List<TagTopicInfo> response2 = List.of(TagTopicInfo.builder()
            .entityName(tagTopicInfo.entityName())
            .entityStatus("")
            .typeName(TAG1)
            .entityType(TOPIC_ENTITY_TYPE)
            .build());

        when(schemaRegistryClient.createTags(any(), any())).thenReturn(Mono.just(response));
        when(schemaRegistryClient.associateTags(any(), any())).thenReturn(Mono.just(response2));

        topicAsyncExecutor.createAndAssociateTags(topicTagsMapping);

        verify(topicRepository).create(topic);
    }

    @Test
    void shouldDeleteTopicNoTags() throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
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
        when(managedClusterProperties.isConfluentCloud()).thenReturn(false);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
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

        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
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
            .metadata(Metadata.builder()
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
        when(managedClusterProperties.isConfluentCloud()).thenReturn(false);

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(Metadata.builder()
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
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
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
                .metadata(Metadata.builder()
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
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);

        when(schemaRegistryClient.getTopicWithTags(LOCAL_CLUSTER))
            .thenReturn(Mono.empty());

        Map<String, Topic> brokerTopics = Map.of(TOPIC_NAME,
            Topic.builder()
                .metadata(Metadata.builder()
                    .name(TOPIC_NAME)
                    .build())
                .spec(Topic.TopicSpec.builder()
                    .build())
                .build());

        topicAsyncExecutor.enrichWithTags(brokerTopics);

        assertTrue(brokerTopics.get(TOPIC_NAME).getSpec().getTags().isEmpty());
    }

    @Test
    void shouldNotUpdateSameDescriptionWhenSucceed() {
        List<Topic> ns4kafkaTopics = List.of(
            Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION1)
                .build())
            .build());

        Map<String, Topic> brokerTopics = Map.of(
            TOPIC_NAME, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .generation(0)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION1)
                .build())
            .build());

        topicAsyncExecutor.alterDescriptions(ns4kafkaTopics, brokerTopics);

        assertEquals(0, brokerTopics.get(TOPIC_NAME).getMetadata().getGeneration());
    }

    @Test
    void shouldUpdateDescriptionWhenSucceed() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION1)
                .build())
            .build();

        List<Topic> ns4kafkaTopics = List.of(topic);

        Map<String, Topic> brokerTopics = Map.of(
            TOPIC_NAME, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .generation(0)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION2)
                .build())
            .build());

        TopicDescriptionUpdateResponse response = new TopicDescriptionUpdateResponse(null);

        when(managedClusterProperties.getConfig()).thenReturn(properties);
        when(schemaRegistryClient.updateDescription(any(), any())).thenReturn(Mono.just(response));

        topicAsyncExecutor.alterDescriptions(ns4kafkaTopics, brokerTopics);

        verify(topicRepository).create(topic);
    }

    @Test
    void shouldUpdateDescriptionWhenFail() {
        Properties properties = new Properties();
        properties.put(CLUSTER_ID, CLUSTER_ID_TEST);

        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION1)
                .build())
            .build();

        List<Topic> ns4kafkaTopics = List.of(topic);

        Map<String, Topic> brokerTopics = Map.of(
            TOPIC_NAME, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .generation(0)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION2)
                .build())
            .build());

        when(managedClusterProperties.getConfig()).thenReturn(properties);
        when(schemaRegistryClient.updateDescription(any(), any())).thenReturn(Mono.error(new IOException()));

        topicAsyncExecutor.alterDescriptions(ns4kafkaTopics, brokerTopics);

        verify(topicRepository).create(topic);
    }

    @Test
    void shouldNotEnrichWithDescriptionWhenNotConfluentCloud() {
        when(managedClusterProperties.isConfluentCloud()).thenReturn(false);

        Map<String, Topic> brokerTopics = Map.of(
            TOPIC_NAME, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .build())
            .build(),

            TOPIC_NAME2, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME2)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION1)
                .build())
            .build());

        topicAsyncExecutor.enrichWithDescription(brokerTopics);

        assertNull(brokerTopics.get(TOPIC_NAME).getSpec().getDescription());
        assertEquals(DESCRIPTION1, brokerTopics.get(TOPIC_NAME2).getSpec().getDescription());
    }

    @Test
    void shouldEnrichWithDescriptionWhenConfluentCloud() {
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);

        TopicListResponseEntityAttributes attributes = TopicListResponseEntityAttributes.builder()
            .name(TOPIC_NAME)
            .description(DESCRIPTION1).build();
        TopicListResponseEntity entity = TopicListResponseEntity.builder().attributes(attributes).build();
        TopicListResponse response1 = TopicListResponse.builder().entities(List.of(entity)).build();
        TopicListResponse response2 = TopicListResponse.builder().entities(List.of()).build();

        when(schemaRegistryClient.getTopicWithDescription(LOCAL_CLUSTER, 1000, 0))
            .thenReturn(Mono.just(response1));
        when(schemaRegistryClient.getTopicWithDescription(LOCAL_CLUSTER, 1000, 1000))
            .thenReturn(Mono.just(response2));

        Map<String, Topic> brokerTopics = Map.of(
            TOPIC_NAME, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION2)
                .build())
            .build());

        topicAsyncExecutor.enrichWithDescription(brokerTopics);

        assertEquals(DESCRIPTION1, brokerTopics.get(TOPIC_NAME).getSpec().getDescription());
    }

    @Test
    void shouldEnrichWithDescriptionForMultipleTopics() {
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);

        TopicListResponseEntityAttributes attributes1 = TopicListResponseEntityAttributes.builder()
            .name(TOPIC_NAME)
            .description(DESCRIPTION1).build();
        TopicListResponseEntityAttributes attributes2 = TopicListResponseEntityAttributes.builder()
            .name(TOPIC_NAME2)
            .description(DESCRIPTION2).build();
        TopicListResponseEntityAttributes attributes3 = TopicListResponseEntityAttributes.builder()
            .name(TOPIC_NAME3)
            .build();
        TopicListResponseEntity entity1 = TopicListResponseEntity.builder().attributes(attributes1).build();
        TopicListResponseEntity entity2 = TopicListResponseEntity.builder().attributes(attributes2).build();
        TopicListResponseEntity entity3 = TopicListResponseEntity.builder().attributes(attributes3).build();
        TopicListResponse response1 = TopicListResponse.builder().entities(List.of(entity1, entity2, entity3)).build();
        TopicListResponse response2 = TopicListResponse.builder().entities(List.of()).build();

        when(schemaRegistryClient.getTopicWithDescription(LOCAL_CLUSTER, 1000, 0))
            .thenReturn(Mono.just(response1));
        when(schemaRegistryClient.getTopicWithDescription(LOCAL_CLUSTER, 1000, 1000))
            .thenReturn(Mono.just(response2));

        Map<String, Topic> brokerTopics = Map.of(
            TOPIC_NAME, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .build())
            .build(),

            TOPIC_NAME2, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME2)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION1)
                .build())
            .build(),

            TOPIC_NAME3, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME3)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION2)
                .build())
            .build());

        topicAsyncExecutor.enrichWithDescription(brokerTopics);

        assertEquals(DESCRIPTION1, brokerTopics.get(TOPIC_NAME).getSpec().getDescription());
        assertEquals(DESCRIPTION2, brokerTopics.get(TOPIC_NAME2).getSpec().getDescription());
        assertNull(brokerTopics.get(TOPIC_NAME3).getSpec().getDescription());
    }

    @Test
    void shouldEnrichWithDescriptionWhenConfluentCloudAndResponseIsNull() {
        when(managedClusterProperties.isConfluentCloud()).thenReturn(true);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(schemaRegistryClient.getTopicWithDescription(LOCAL_CLUSTER, 1000, 0))
            .thenReturn(Mono.empty());

        Map<String, Topic> brokerTopics = Map.of(
            TOPIC_NAME, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME)
                .build())
            .spec(Topic.TopicSpec.builder()
                .build())
            .build(),

            TOPIC_NAME2, Topic.builder()
            .metadata(Metadata.builder()
                .name(TOPIC_NAME2)
                .build())
            .spec(Topic.TopicSpec.builder()
                .description(DESCRIPTION1)
                .build())
            .build());

        topicAsyncExecutor.enrichWithDescription(brokerTopics);

        assertNull(brokerTopics.get(TOPIC_NAME).getSpec().getDescription());
        assertNull(brokerTopics.get(TOPIC_NAME2).getSpec().getDescription());
    }
}
