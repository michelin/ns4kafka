package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ResourceQuotaServiceTest {
    /**
     * The resource quota service
     */
    @InjectMocks
    ResourceQuotaService resourceQuotaService;

    /**
     * The resource quota repository
     */
    @Mock
    ResourceQuotaRepository resourceQuotaRepository;

    /**
     * The topic service
     */
    @Mock
    TopicService topicService;

    /**
     * Connect service
     */
    @Mock
    KafkaConnectService kafkaConnectService;

    /**
     * Test get quota by namespace when it is defined
     */
    @Test
    void findByNamespace() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "1"))
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.of(resourceQuota));

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByNamespace(ns.getMetadata().getName());
        Assertions.assertTrue(resourceQuotaOptional.isPresent());
        Assertions.assertEquals("test", resourceQuotaOptional.get().getMetadata().getName());
    }

    /**
     * Test get quota by namespace when it is empty
     */
    @Test
    void findByNamespaceEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.empty());

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByNamespace(ns.getMetadata().getName());
        Assertions.assertTrue(resourceQuotaOptional.isEmpty());
    }

    /**
     * Test get quota by name
     */
    @Test
    void findByName() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "1"))
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.of(resourceQuota));

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByName(ns.getMetadata().getName(), "test");
        Assertions.assertTrue(resourceQuotaOptional.isPresent());
        Assertions.assertEquals("test", resourceQuotaOptional.get().getMetadata().getName());
    }

    /**
     * Test get quota by wrong name
     */
    @Test
    void findByNameWrongName() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "1"))
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.of(resourceQuota));

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByName(ns.getMetadata().getName(), "wrong-name");
        Assertions.assertTrue(resourceQuotaOptional.isEmpty());
    }

    /**
     * Test get quota when there is no quota defined on the namespace
     */
    @Test
    void findByNameEmpty() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.empty());

        Optional<ResourceQuota> resourceQuotaOptional = resourceQuotaService.findByName(ns.getMetadata().getName(), "test");
        Assertions.assertTrue(resourceQuotaOptional.isEmpty());
    }

    /**
     * Test create quota
     */
    @Test
    void create() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "1"))
                .build();

        when(resourceQuotaRepository.create(resourceQuota))
                .thenReturn(resourceQuota);

        ResourceQuota createdResourceQuota = resourceQuotaService.create(resourceQuota);
        Assertions.assertEquals(resourceQuota, createdResourceQuota);
        verify(resourceQuotaRepository, times(1)).create(resourceQuota);
    }

    /**
     * Test delete quota
     */
    @Test
    void delete() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "1"))
                .build();

        doNothing().when(resourceQuotaRepository).delete(resourceQuota);

        resourceQuotaService.delete(resourceQuota);
        verify(resourceQuotaRepository, times(1)).delete(resourceQuota);
    }

    /**
     * Test successful validation when creating quota
     */
    @Test
    void validateNewQuotaAgainstCurrentResourceSuccess() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "10"))
                .build();

        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateNewQuotaAgainstCurrentResource(ns, resourceQuota);
        Assertions.assertEquals(0, validationErrors.size());
    }

    /**
     * Test validation when creating quota on count/topics
     */
    @Test
    void validateNewQuotaAgainstCurrentResourceForCountTopics() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "2"))
                .build();

        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateNewQuotaAgainstCurrentResource(ns, resourceQuota);
        Assertions.assertEquals(1, validationErrors.size());
        Assertions.assertEquals("Quota already exceeded for count/topics: 3/2 (used/limit)", validationErrors.get(0));
    }

    /**
     * Test validation when creating quota on count/partitions
     */
    @Test
    void validateNewQuotaAgainstCurrentResourceForCountPartitions() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_PARTITIONS.toString(), "10"))
                .build();

        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(3)
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(10)
                        .build())
                .build();

        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateNewQuotaAgainstCurrentResource(ns, resourceQuota);
        Assertions.assertEquals(1, validationErrors.size());
        Assertions.assertEquals("Quota already exceeded for count/partitions: 19/10 (used/limit)", validationErrors.get(0));
    }

    /**
     * Test validation when creating quota on count/connectors
     */
    @Test
    void validateNewQuotaAgainstCurrentResourceForCountConnectors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_CONNECTORS.toString(), "1"))
                .build();

        when(kafkaConnectService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()));

        List<String> validationErrors = resourceQuotaService.validateNewQuotaAgainstCurrentResource(ns, resourceQuota);
        Assertions.assertEquals(1, validationErrors.size());
        Assertions.assertEquals("Quota already exceeded for count/connectors: 2/1 (used/limit)", validationErrors.get(0));
    }

    /**
     * Test get current used resource for count topics
     */
    @Test
    void getCurrentUsedResourceForCountTopics() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));

        Integer currentlyUsed = resourceQuotaService.getCurrentUsedResource(ns, COUNT_TOPICS);
        Assertions.assertEquals(3, currentlyUsed);
    }

    /**
     * Test get current used resource for count partitions
     */
    @Test
    void getCurrentUsedResourceForCountPartitions() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(3)
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(10)
                        .build())
                .build();

        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));

        Integer currentlyUsed = resourceQuotaService.getCurrentUsedResource(ns, COUNT_PARTITIONS);
        Assertions.assertEquals(19, currentlyUsed);
    }

    /**
     * Test get current used resource for count connectors
     */
    @Test
    void getCurrentUsedResourceForCountConnectors() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(kafkaConnectService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()));

        Integer currentlyUsed = resourceQuotaService.getCurrentUsedResource(ns, COUNT_CONNECTORS);
        Assertions.assertEquals(2, currentlyUsed);
    }

    /**
     * Test get current used resource for count topics
     */
    @Test
    void getCurrentUsedResourceForCountTopics() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .build();

        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));

        Integer currentlyUsed = resourceQuotaService.getCurrentUsedResource(ns, COUNT_TOPICS);
        Assertions.assertEquals(3, currentlyUsed);
    }

    /**
     * Test quota validation on topics
     */
    @Test
    void validateTopicQuota() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "4"))
                .spec(Map.of(COUNT_PARTITIONS.toString(), "25"))
                .build();

        Topic newTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();


        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(3)
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(10)
                        .build())
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.of(resourceQuota));
        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateTopicQuota(ns, newTopic);
        Assertions.assertEquals(0, validationErrors.size());
    }

    /**
     * Test quota validation on topics when there is no quota defined
     */
    @Test
    void validateTopicQuotaNoQuota() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic newTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.empty());

        List<String> validationErrors = resourceQuotaService.validateTopicQuota(ns, newTopic);
        Assertions.assertEquals(0, validationErrors.size());
    }

    /**
     * Test quota validation on topics when quota is being exceeded
     */
    @Test
    void validateTopicQuotaExceed() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "3", COUNT_PARTITIONS.toString(), "20"))
                .build();

        Topic newTopic = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();


        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(3)
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(10)
                        .build())
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.of(resourceQuota));
        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateTopicQuota(ns, newTopic);
        Assertions.assertEquals(2, validationErrors.size());
        Assertions.assertEquals("Exceeding quota for count/topics: 3/3 (used/limit). Cannot add 1 topic.", validationErrors.get(0));
        Assertions.assertEquals("Exceeding quota for count/partitions: 19/20 (used/limit). Cannot add 6 partition(s).", validationErrors.get(1));
    }

    /**
     * Test quota validation on connectors
     */
    @Test
    void validateConnectorQuota() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_CONNECTORS.toString(), "3"))
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.of(resourceQuota));
        when(kafkaConnectService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()));

        List<String> validationErrors = resourceQuotaService.validateConnectorQuota(ns);
        Assertions.assertEquals(0, validationErrors.size());
    }

    /**
     * Test quota validation on connectors when there is no quota defined
     */
    @Test
    void validateConnectorQuotaNoQuota() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.empty());

        List<String> validationErrors = resourceQuotaService.validateConnectorQuota(ns);
        Assertions.assertEquals(0, validationErrors.size());
    }

    /**
     * Test quota validation on connectors when quota is being exceeded
     */
    @Test
    void validateConnectorQuotaExceed() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_CONNECTORS.toString(), "2"))
                .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
                .thenReturn(Optional.of(resourceQuota));
        when(kafkaConnectService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()));

        List<String> validationErrors = resourceQuotaService.validateConnectorQuota(ns);
        Assertions.assertEquals(1, validationErrors.size());
        Assertions.assertEquals("Exceeding quota for count/connectors: 2/2 (used/limit). Cannot add 1 connector.", validationErrors.get(0));
    }

    /**
     * Test response format
     */
    @Test
    void toResponse() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .cluster("local")
                        .name("test")
                        .build())
                .spec(Map.of(COUNT_TOPICS.toString(), "3",
                        COUNT_PARTITIONS.toString(), "20",
                        COUNT_CONNECTORS.toString(), "2"))
                .build();

        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(3)
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(10)
                        .build())
                .build();

        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));
        when(kafkaConnectService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()));

        ResourceQuotaResponse response = resourceQuotaService.toResponse(ns, Optional.of(resourceQuota));
        Assertions.assertEquals(resourceQuota.getMetadata(), response.getMetadata());
        Assertions.assertEquals("3/3", response.getSpec().getCountTopic());
        Assertions.assertEquals("19/20", response.getSpec().getCountPartition());
        Assertions.assertEquals("2/2", response.getSpec().getCountConnector());
    }

    /**
     * Test response format when there is no quota
     */
    @Test
    void toResponseNoQuota() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .connectClusters(List.of("local-name"))
                        .build())
                .build();

        Topic topic1 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(6)
                        .build())
                .build();

        Topic topic2 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(3)
                        .build())
                .build();

        Topic topic3 = Topic.builder()
                .metadata(ObjectMeta.builder()
                        .name("topic")
                        .namespace("namespace")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(10)
                        .build())
                .build();

        when(topicService.findAllForNamespace(ns))
                .thenReturn(List.of(topic1, topic2, topic3));
        when(kafkaConnectService.findAllForNamespace(ns))
                .thenReturn(List.of(
                        Connector.builder().metadata(ObjectMeta.builder().name("connect1").build()).build(),
                        Connector.builder().metadata(ObjectMeta.builder().name("connect2").build()).build()));

        ResourceQuotaResponse response = resourceQuotaService.toResponse(ns, Optional.empty());
        Assertions.assertNull(response.getMetadata());
        Assertions.assertEquals("3", response.getSpec().getCountTopic());
        Assertions.assertEquals("19", response.getSpec().getCountPartition());
        Assertions.assertEquals("2", response.getSpec().getCountConnector());
    }
}
