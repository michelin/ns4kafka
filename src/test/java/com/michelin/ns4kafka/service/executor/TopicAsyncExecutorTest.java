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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.TopicRepository;
import com.michelin.ns4kafka.service.TopicService;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicAsyncExecutorTest {
    private static final String LOCAL_CLUSTER = "local";
    private static final String TOPIC_NAME = "topic";
    private static final Instant instant = Instant.parse("2026-01-01T00:00:00Z");

    @Mock
    ManagedClusterProperties managedClusterProperties;

    @Mock
    TopicRepository topicRepository;

    @Mock
    TopicService topicService;

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
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(topic));

        topicAsyncExecutor.createTopics(List.of(topic));

        verify(topicRepository).create(argThat(a -> a.equals(topic) && a.isSuccess() && a.isCreated()));
    }

    @Test
    void shouldCreateTopicButNotUpdateStatusWhenChangedSinceLastApply() {
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
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        Topic newTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().configs(Map.of("key", "value")).build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(newTopic));

        topicAsyncExecutor.createTopics(List.of(topic));

        verify(topicRepository).create(argThat(a -> a.equals(newTopic) && a.isPending() && a.isCreated()));
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
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(topic));

        topicAsyncExecutor.createTopics(List.of(topic));

        verify(topicRepository).create(argThat(a -> a.equals(topic) && a.isFailed() && !a.isCreated()));
    }

    @Test
    void shouldNotCreateTopicWhenErrorCreatingAndChangedSinceLastApply()
            throws ExecutionException, InterruptedException, TimeoutException {
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
                        .updateTimestamp(Date.from(instant))
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        Topic newTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(0)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(newTopic));

        topicAsyncExecutor.createTopics(List.of(topic));

        verify(topicRepository, never()).create(any());
    }

    @Test
    void shouldDeleteTopics() {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.topicNameValues()).thenReturn(Map.of("topic", kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setDelete(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(topic));

        topicAsyncExecutor.deleteTopics(List.of(topic));

        verify(topicRepository).delete(topic);
        verify(topicRepository, never()).create(topic);
    }

    @Test
    void shouldNotDeleteTopicWhenChangedSinceLastApply() {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.topicNameValues()).thenReturn(Map.of("topic", kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setDelete(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        Topic newTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(newTopic));

        topicAsyncExecutor.deleteTopics(List.of(topic));

        verify(topicRepository, never()).create(any());
        verify(topicRepository, never()).delete(any());
    }

    @Test
    void shouldNotDeleteTopicAndUpdateStatusWhenExecutionError()
            throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.topicNameValues()).thenReturn(Map.of("topic", kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setDelete(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("Error", new Exception()));

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(topic));

        topicAsyncExecutor.deleteTopics(List.of(topic));

        verify(topicRepository).create(argThat(a -> a.equals(topic) && a.isFailed()));
        verify(topicRepository, never()).delete(any());
    }

    @Test
    void shouldDeleteTopicWhenNotExistInCluster() throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.topicNameValues()).thenReturn(Map.of("topic", kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setDelete(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS))
                .thenThrow(new ExecutionException("Error", new UnknownTopicOrPartitionException()));

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(topic));

        topicAsyncExecutor.deleteTopics(List.of(topic));

        verify(topicRepository).delete(topic);
        verify(topicRepository, never()).create(any());
    }

    @Test
    void shouldNotDeleteTopicAndNotUpdateStatusWhenExecutionErrorAndChangedSinceLastApply()
            throws ExecutionException, InterruptedException, TimeoutException {
        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.deleteTopics(anyList())).thenReturn(deleteTopicsResult);
        when(deleteTopicsResult.topicNameValues()).thenReturn(Map.of("topic", kafkaFuture));

        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setDelete(1000);

        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getTimeout()).thenReturn(timeoutProperties);
        when(kafkaFuture.get(1000, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException("Error", new Exception()));

        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofDeleting())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        Topic newTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(newTopic));

        topicAsyncExecutor.deleteTopics(List.of(topic));

        verify(topicRepository, never()).delete(any());
        verify(topicRepository, never()).create(any());
    }

    @Test
    void shouldNotUpdateTopicConfigsWhenChangedSinceLastApply() {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, "topic");
        ManagedClusterProperties.TimeoutProperties.TopicProperties topicProperties =
                new ManagedClusterProperties.TimeoutProperties.TopicProperties();
        topicProperties.setAlterConfigs(1000);
        ManagedClusterProperties.TimeoutProperties timeoutProperties = new ManagedClusterProperties.TimeoutProperties();
        timeoutProperties.setTopic(topicProperties);

        when(managedClusterProperties.getAdminClient()).thenReturn(adminClient);
        when(adminClient.incrementalAlterConfigs(any())).thenReturn(alterConfigsResult);
        when(alterConfigsResult.values()).thenReturn(Map.of(cr, kafkaFuture));

        Topic newTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(newTopic));

        topicAsyncExecutor.alterTopics(List.of(topic), Map.of("topic", topic));

        verify(topicRepository, never()).create(any());
    }

    @Test
    void shouldNotUpdateTopicConfigsWhenErrorUpdating()
            throws ExecutionException, InterruptedException, TimeoutException {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, "topic");
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
        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(topic));

        topicAsyncExecutor.alterTopics(List.of(topic), Map.of("topic", topic));

        verify(topicRepository).create(argThat(a -> a.equals(topic) && a.isFailed()));
    }

    @Test
    void shouldNotUpdateTopicConfigsWhenErrorUpdatingAndChangedSinceLastSupply()
            throws ExecutionException, InterruptedException, TimeoutException {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, "topic");
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

        Topic newTopic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .cluster("local")
                        .name("topic")
                        .status(Resource.Metadata.Status.ofPending())
                        .updateTimestamp(Date.from(instant.plusSeconds(1)))
                        .generation(1)
                        .build())
                .spec(Topic.TopicSpec.builder().build())
                .build();

        when(topicService.findByName("local", "topic")).thenReturn(Optional.of(newTopic));

        topicAsyncExecutor.alterTopics(List.of(topic), Map.of("topic", topic));

        verify(topicRepository, never()).create(any());
    }
}
