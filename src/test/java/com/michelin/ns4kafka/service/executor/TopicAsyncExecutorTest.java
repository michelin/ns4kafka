/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.service.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TopicRepository;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicAsyncExecutorTest {
    private static final String LOCAL_CLUSTER = "local";
    private static final String TOPIC_NAME = "topic";

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @Mock
    TopicRepository topicRepository;

    @Mock
    Admin adminClient;

    @Mock
    CreateTopicsResult createTopicsResult;

    @Mock
    DeleteTopicsResult deleteTopicsResult;

    @Mock
    AlterConfigsResult alterConfigsResult;

    @Mock
    KafkaFuture<Void> kafkaFuture;

    @InjectMocks
    TopicAsyncExecutor topicAsyncExecutor;

    @Test
    void shouldSynchronizeTopicsBasedOnBrokerStateRegardlessOfStatus() throws Exception {
        Topic topicToCreate = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name("topic-to-create")
                        .generation(3)
                        .status(Resource.Metadata.Status.ofSuccess())
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "delete"))
                        .build())
                .build();
        Topic topicToUpdate = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .generation(2)
                        .status(Resource.Metadata.Status.ofSuccess())
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "compact"))
                        .build())
                .build();
        Topic brokerTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "delete"))
                        .build())
                .build();
        Topic brokerOnlyStreamTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name("application-changelog")
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();
        Map<String, Topic> brokerTopics =
                Map.of(TOPIC_NAME, brokerTopic, "application-changelog", brokerOnlyStreamTopic);
        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);

        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(topicRepository.findAllForCluster(LOCAL_CLUSTER)).thenReturn(List.of(topicToCreate, topicToUpdate));
        when(topicRepository.findByName(LOCAL_CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(topicToUpdate));
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.incrementalAlterConfigs(any())).thenReturn(alterConfigsResult);
        when(alterConfigsResult.values()).thenReturn(Map.of(topicResource, kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setAlterConfigs(1000);
        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);
        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);

        TopicAsyncExecutor executor = spy(topicAsyncExecutor);
        doReturn(List.of(TOPIC_NAME, "application-changelog")).when(executor).listBrokerTopicNames();
        doReturn(brokerTopics)
                .when(executor)
                .collectBrokerTopicsFromNames(List.of(TOPIC_NAME, "application-changelog"));
        doNothing().when(executor).createTopics(anyList());

        executor.synchronizeTopics();

        verify(executor).createTopics(List.of(topicToCreate));
        verify(adminClient)
                .incrementalAlterConfigs(argThat(
                        configChanges -> configChanges.size() == 1 && configChanges.containsKey(topicResource)));
        verify(topicRepository)
                .create(argThat(updated -> updated == topicToUpdate
                        && updated.isSuccess()
                        && updated.getMetadata().getGeneration() == 3));
        // Kafka Streams internal topics are no longer imported from the broker
        verify(topicRepository, never())
                .create(argThat(topic ->
                        "application-changelog".equals(topic.getMetadata().getName())));
    }

    @Test
    void shouldCollectBrokerTopicsFromNames() throws Exception {
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
        TopicDescription topicDescription = mock(TopicDescription.class);
        TopicPartitionInfo partitionInfo = mock(TopicPartitionInfo.class);
        Config config = mock(Config.class);
        ConfigEntry configEntry = mock(ConfigEntry.class);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(adminClient.describeTopics(List.of(TOPIC_NAME))).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames())
                .thenReturn(KafkaFuture.completedFuture(Map.of(TOPIC_NAME, topicDescription)));
        when(topicDescription.partitions()).thenReturn(List.of(partitionInfo));
        when(partitionInfo.replicas())
                .thenReturn(List.of(new Node(1, "broker-1", 9092), new Node(2, "broker-2", 9092)));
        when(adminClient.describeConfigs(List.of(configResource))).thenReturn(describeConfigsResult);
        when(describeConfigsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(configResource, config)));
        when(config.entries()).thenReturn(List.of(configEntry));
        when(configEntry.source()).thenReturn(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
        when(configEntry.name()).thenReturn("cleanup.policy");
        when(configEntry.value()).thenReturn("compact");

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setDescribeConfigs(1000);
        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);
        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);

        Map<String, Topic> topics = topicAsyncExecutor.collectBrokerTopicsFromNames(List.of(TOPIC_NAME));

        assertEquals(1, topics.get(TOPIC_NAME).getSpec().getPartitions());
        assertEquals(2, topics.get(TOPIC_NAME).getSpec().getReplicationFactor());
        assertEquals(
                Map.of("cleanup.policy", "compact"),
                topics.get(TOPIC_NAME).getSpec().getConfigs());
    }

    @Test
    void shouldCreateTopics() {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.createTopics(anyList())).thenReturn(createTopicsResult);
        when(createTopicsResult.values()).thenReturn(Map.of("topic", kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setCreate(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(topicRepository.findByName(LOCAL_CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(topic));

        topicAsyncExecutor.createTopics(List.of(topic));

        verify(topicRepository).create(argThat(a -> a.equals(topic) && a.isSuccess() && a.isCreated()));
    }

    @Test
    void shouldUpdateStatusWhenErrorCreating() throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.createTopics(anyList())).thenReturn(createTopicsResult);
        when(createTopicsResult.values()).thenReturn(Map.of("topic", kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setCreate(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("Error", new Throwable()));

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(topicRepository.findByName(LOCAL_CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(topic));

        topicAsyncExecutor.createTopics(List.of(topic));

        verify(topicRepository).create(argThat(a -> a.equals(topic) && a.isFailed() && !a.isCreated()));
    }

    @Test
    void shouldDeleteTopics() throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setDelete(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);

        Topic topic1 = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name("topic2")
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        topicAsyncExecutor.deleteTopics(List.of(topic1, topic2));

        verify(adminClient).deleteTopics(List.of(TOPIC_NAME, "topic2"));
        verify(kafkaFuture).get(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldThrowExceptionWhenDeletingTopicsFails()
            throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.all()).thenReturn(kafkaFuture);

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setDelete(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("Error", new Exception()));

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        List<Topic> topics = List.of(topic);

        assertThrows(ExecutionException.class, () -> topicAsyncExecutor.deleteTopics(topics));
        verify(topicRepository, never()).delete(any());
    }

    @Test
    void shouldNotUpdateTopicConfigsWhenErrorUpdating()
            throws ExecutionException, InterruptedException, TimeoutException {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .status(Resource.Metadata.Status.ofPending())
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("retention.ms", "60000"))
                        .build())
                .build();

        Topic brokerTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setAlterConfigs(1000);
        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.incrementalAlterConfigs(any())).thenReturn(alterConfigsResult);
        when(alterConfigsResult.values()).thenReturn(Map.of(cr, kafkaFuture));
        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("Error", new Exception()));

        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(topicRepository.findAllForCluster(LOCAL_CLUSTER)).thenReturn(List.of(topic));
        when(topicRepository.findByName(LOCAL_CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(topic));

        TopicAsyncExecutor executor = spy(topicAsyncExecutor);
        doReturn(List.of(TOPIC_NAME)).when(executor).listBrokerTopicNames();
        doReturn(Map.of(TOPIC_NAME, brokerTopic)).when(executor).collectBrokerTopicsFromNames(List.of(TOPIC_NAME));

        executor.synchronizeTopics();

        verify(topicRepository).create(argThat(a -> a.equals(topic) && a.isFailed()));
    }

    @Test
    void shouldNotPersistCreatedTopicWhenDeletedDuringSynchronization() {
        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setCreate(1000);
        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(adminClient.createTopics(anyList())).thenReturn(createTopicsResult);
        when(createTopicsResult.values()).thenReturn(Map.of(TOPIC_NAME, kafkaFuture));
        when(topicRepository.findByName(LOCAL_CLUSTER, TOPIC_NAME)).thenReturn(Optional.empty());

        topicAsyncExecutor.createTopics(List.of(topic));

        verify(topicRepository, never()).create(any());
    }

    @Test
    void shouldNotPersistUpdatedTopicWhenReappliedDuringSynchronization() throws Exception {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .updateTimestamp(new Date(1000))
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("retention.ms", "60000"))
                        .build())
                .build();

        Topic reappliedTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .updateTimestamp(new Date(2000))
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("retention.ms", "120000"))
                        .build())
                .build();

        Topic brokerTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setAlterConfigs(1000);
        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(adminClient.incrementalAlterConfigs(any())).thenReturn(alterConfigsResult);
        when(alterConfigsResult.values()).thenReturn(Map.of(cr, kafkaFuture));
        when(topicRepository.findAllForCluster(LOCAL_CLUSTER)).thenReturn(List.of(topic));
        when(topicRepository.findByName(LOCAL_CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(reappliedTopic));

        TopicAsyncExecutor executor = spy(topicAsyncExecutor);
        doReturn(List.of(TOPIC_NAME)).when(executor).listBrokerTopicNames();
        doReturn(Map.of(TOPIC_NAME, brokerTopic)).when(executor).collectBrokerTopicsFromNames(List.of(TOPIC_NAME));

        executor.synchronizeTopics();

        verify(topicRepository, never()).create(any());
    }

    @Test
    void shouldNotCallBrokerWhenNoConfigChanges() throws Exception {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .status(Resource.Metadata.Status.ofSuccess())
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "delete, compact"))
                        .build())
                .build();

        Topic brokerTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "compact,delete"))
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(topicRepository.findAllForCluster(LOCAL_CLUSTER)).thenReturn(List.of(topic));

        TopicAsyncExecutor executor = spy(topicAsyncExecutor);
        doReturn(List.of(TOPIC_NAME)).when(executor).listBrokerTopicNames();
        doReturn(Map.of(TOPIC_NAME, brokerTopic)).when(executor).collectBrokerTopicsFromNames(List.of(TOPIC_NAME));

        executor.synchronizeTopics();

        verify(adminClient, never()).incrementalAlterConfigs(any());
        verify(topicRepository, never()).create(any());
    }

    @ParameterizedTest
    @MethodSource("unresolvedStatuses")
    void shouldResolveStatusWhenNoConfigChanges(Resource.Metadata.Status status) throws Exception {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .status(status)
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "delete"))
                        .build())
                .build();

        Topic brokerTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "delete"))
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(topicRepository.findAllForCluster(LOCAL_CLUSTER)).thenReturn(List.of(topic));
        when(topicRepository.findByName(LOCAL_CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(topic));

        TopicAsyncExecutor executor = spy(topicAsyncExecutor);
        doReturn(List.of(TOPIC_NAME)).when(executor).listBrokerTopicNames();
        doReturn(Map.of(TOPIC_NAME, brokerTopic)).when(executor).collectBrokerTopicsFromNames(List.of(TOPIC_NAME));

        executor.synchronizeTopics();

        verify(adminClient, never()).incrementalAlterConfigs(any());
        verify(topicRepository)
                .create(argThat(resolved -> resolved == topic
                        && resolved.isSuccess()
                        && resolved.getMetadata().getGeneration() == 2));
    }

    static Stream<Resource.Metadata.Status> unresolvedStatuses() {
        return Stream.of(Resource.Metadata.Status.ofPending(), Resource.Metadata.Status.ofFailed("Error"), null);
    }

    @Test
    void shouldNotResolveStatusWhenReappliedDuringSynchronization() throws Exception {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(new Date(1000))
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "delete"))
                        .build())
                .build();

        Topic reappliedTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(new Date(2000))
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "compact"))
                        .build())
                .build();

        Topic brokerTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster(LOCAL_CLUSTER)
                        .name(TOPIC_NAME)
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .configs(Map.of("cleanup.policy", "delete"))
                        .build())
                .build();

        when(managedClusterProperties.getName()).thenReturn(LOCAL_CLUSTER);
        when(topicRepository.findAllForCluster(LOCAL_CLUSTER)).thenReturn(List.of(topic));
        when(topicRepository.findByName(LOCAL_CLUSTER, TOPIC_NAME)).thenReturn(Optional.of(reappliedTopic));

        TopicAsyncExecutor executor = spy(topicAsyncExecutor);
        doReturn(List.of(TOPIC_NAME)).when(executor).listBrokerTopicNames();
        doReturn(Map.of(TOPIC_NAME, brokerTopic)).when(executor).collectBrokerTopicsFromNames(List.of(TOPIC_NAME));

        executor.synchronizeTopics();

        verify(topicRepository, never()).create(any());
    }
}
