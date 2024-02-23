package com.michelin.ns4kafka.services;

import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.COUNT_CONNECTORS;
import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.COUNT_PARTITIONS;
import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.COUNT_TOPICS;
import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.DISK_TOPICS;
import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.USER_CONSUMER_BYTE_RATE;
import static com.michelin.ns4kafka.models.quota.ResourceQuota.ResourceQuotaSpecKey.USER_PRODUCER_BYTE_RATE;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.models.quota.ResourceQuotaResponse;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ResourceQuotaServiceTest {
    @InjectMocks
    ResourceQuotaService resourceQuotaService;

    @Mock
    ResourceQuotaRepository resourceQuotaRepository;

    @Mock
    TopicService topicService;

    @Mock
    ConnectorService connectorService;

    @Test
    void findByNamespace() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "1"))
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));

        Optional<ResourceQuota> resourceQuotaOptional =
            resourceQuotaService.findByNamespace(ns.getMetadata().getName());
        assertTrue(resourceQuotaOptional.isPresent());
        assertEquals("test", resourceQuotaOptional.get().getMetadata().getName());
    }

    @Test
    void findByNamespaceEmpty() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.empty());

        Optional<ResourceQuota> resourceQuotaOptional =
            resourceQuotaService.findByNamespace(ns.getMetadata().getName());
        assertTrue(resourceQuotaOptional.isEmpty());
    }

    @Test
    void findByName() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "1"))
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));

        Optional<ResourceQuota> resourceQuotaOptional =
            resourceQuotaService.findByName(ns.getMetadata().getName(), "test");
        assertTrue(resourceQuotaOptional.isPresent());
        assertEquals("test", resourceQuotaOptional.get().getMetadata().getName());
    }

    @Test
    void findByNameWrongName() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "1"))
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));

        Optional<ResourceQuota> resourceQuotaOptional =
            resourceQuotaService.findByName(ns.getMetadata().getName(), "wrong-name");
        assertTrue(resourceQuotaOptional.isEmpty());
    }

    @Test
    void findByNameEmpty() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.empty());

        Optional<ResourceQuota> resourceQuotaOptional =
            resourceQuotaService.findByName(ns.getMetadata().getName(), "test");
        assertTrue(resourceQuotaOptional.isEmpty());
    }

    @Test
    void create() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "1"))
            .build();

        when(resourceQuotaRepository.create(resourceQuota))
            .thenReturn(resourceQuota);

        ResourceQuota createdResourceQuota = resourceQuotaService.create(resourceQuota);
        assertEquals(resourceQuota, createdResourceQuota);
        verify(resourceQuotaRepository, times(1)).create(resourceQuota);
    }

    @Test
    void delete() {
        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "1"))
            .build();

        doNothing().when(resourceQuotaRepository).delete(resourceQuota);

        resourceQuotaService.delete(resourceQuota);
        verify(resourceQuotaRepository, times(1)).delete(resourceQuota);
    }

    @Test
    void validateNewQuotaAgainstCurrentResourceSuccess() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "10"))
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, resourceQuota);
        assertEquals(0, validationErrors.size());
    }

    @Test
    void validateNewQuotaAgainstCurrentResourceForCountTopics() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "2"))
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, resourceQuota);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value \"2\" for field \"count/topics\": quota already exceeded (3/2).",
            validationErrors.get(0));
    }

    @Test
    void validateNewQuotaAgainstCurrentResourceForCountPartitions() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_PARTITIONS.toString(), "10"))
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, resourceQuota);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value \"10\" for field \"count/partitions\": quota already exceeded (19/10).",
            validationErrors.get(0));
    }

    @Test
    void validateNewQuotaDiskTopicsFormat() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(DISK_TOPICS.toString(), "10"))
            .build();

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, resourceQuota);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value \"10\" for field \"disk/topics\": value must end with either B, KiB, MiB or GiB.",
            validationErrors.get(0));
    }

    @Test
    void validateNewQuotaAgainstCurrentResourceForDiskTopics() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(DISK_TOPICS.toString(), "5000B"))
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of("retention.bytes", "1000"))
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .configs(Map.of("retention.bytes", "1000"))
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2));

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, resourceQuota);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value \"5000B\" for field \"disk/topics\": quota already exceeded (8.79KiB/5000B).",
            validationErrors.get(0));
    }

    @Test
    void validateNewQuotaAgainstCurrentResourceForCountConnectors() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_CONNECTORS.toString(), "1"))
            .build();

        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of(
                Connector.builder().metadata(Metadata.builder().name("connect1").build()).build(),
                Connector.builder().metadata(Metadata.builder().name("connect2").build()).build()));

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, resourceQuota);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid value \"1\" for field \"count/connectors\": quota already exceeded (2/1).",
            validationErrors.get(0));
    }

    @Test
    void validateUserQuotaFormatError() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(
                Map.of(USER_PRODUCER_BYTE_RATE.toString(), "producer", USER_CONSUMER_BYTE_RATE.toString(), "consumer"))
            .build();

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, resourceQuota);

        assertEquals(2, validationErrors.size());
        assertEquals("Invalid value \"producer\" for field \"user/producer_byte_rate\": value must be a number.",
            validationErrors.get(0));
        assertEquals("Invalid value \"consumer\" for field \"user/consumer_byte_rate\": value must be a number.",
            validationErrors.get(1));
    }

    @Test
    void validateUserQuotaFormatSuccess() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(USER_PRODUCER_BYTE_RATE.toString(), "102400", USER_CONSUMER_BYTE_RATE.toString(), "102400"))
            .build();

        List<String> validationErrors = resourceQuotaService.validateNewResourceQuota(ns, resourceQuota);

        assertEquals(0, validationErrors.size());
    }

    @Test
    void getCurrentUsedResourceForCountTopicsByNamespace() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));

        long currentlyUsed = resourceQuotaService.getCurrentCountTopicsByNamespace(ns);
        assertEquals(3L, currentlyUsed);
    }

    @Test
    void getCurrentUsedResourceForCountPartitionsByNamespace() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));

        long currentlyUsed = resourceQuotaService.getCurrentCountPartitionsByNamespace(ns);
        assertEquals(19L, currentlyUsed);
    }

    @Test
    void getCurrentUsedResourceForCountConnectorsByNamespace() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of(
                Connector.builder().metadata(Metadata.builder().name("connect1").build()).build(),
                Connector.builder().metadata(Metadata.builder().name("connect2").build()).build()));

        long currentlyUsed = resourceQuotaService.getCurrentCountConnectorsByNamespace(ns);
        assertEquals(2L, currentlyUsed);
    }

    @Test
    void getCurrentUsedResourceForDiskTopicsByNamespace() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of("retention.bytes", "1000"))
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .configs(Map.of("retention.bytes", "50000"))
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .configs(Map.of("retention.bytes", "2500"))
                .build())
            .build();

        when(topicService.findAllForNamespace(ns)).thenReturn(List.of(topic1, topic2, topic3));

        long currentlyUsed = resourceQuotaService.getCurrentDiskTopicsByNamespace(ns);
        assertEquals(181000L, currentlyUsed);
    }

    @Test
    void validateTopicQuota() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "4"))
            .spec(Map.of(COUNT_PARTITIONS.toString(), "25"))
            .spec(Map.of(DISK_TOPICS.toString(), "2GiB"))
            .build();

        Topic newTopic = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();


        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));
        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateTopicQuota(ns, Optional.empty(), newTopic);
        assertEquals(0, validationErrors.size());
    }

    @Test
    void validateTopicQuotaNoQuota() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Topic newTopic = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .build())
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.empty());

        List<String> validationErrors = resourceQuotaService.validateTopicQuota(ns, Optional.empty(), newTopic);
        assertEquals(0, validationErrors.size());
    }

    @Test
    void validateTopicQuotaExceed() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "3", COUNT_PARTITIONS.toString(), "20", DISK_TOPICS.toString(),
                "20KiB"))
            .build();

        Topic newTopic = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();


        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));
        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateTopicQuota(ns, Optional.empty(), newTopic);
        assertEquals(3, validationErrors.size());
        assertEquals("Invalid \"apply\" operation: exceeding quota for count/topics: 3/3 (used/limit).",
            validationErrors.get(0));
        assertEquals("Invalid \"apply\" operation: exceeding quota for count/partitions: 19/20 (used/limit). "
            + "Cannot add 6.", validationErrors.get(1));
        assertEquals("Invalid \"apply\" operation: exceeding quota for disk/topics: 18.555KiB/20.0KiB (used/limit). "
                + "Cannot add 5.86KiB.",
            validationErrors.get(2));
    }

    @Test
    void validateUpdateTopicQuotaExceed() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "3", COUNT_PARTITIONS.toString(), "20", DISK_TOPICS.toString(),
                "20KiB"))
            .build();

        Topic newTopic = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1500"))
                .build())
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .configs(Map.of(RETENTION_BYTES_CONFIG, "1000"))
                .build())
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));
        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));

        List<String> validationErrors = resourceQuotaService.validateTopicQuota(ns, Optional.of(topic1), newTopic);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid \"apply\" operation: exceeding quota for disk/topics: 18.555KiB/20.0KiB (used/limit). "
                + "Cannot add 2.93KiB.",
            validationErrors.get(0));
    }

    @Test
    void validateConnectorQuota() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_CONNECTORS.toString(), "3"))
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of(
                Connector.builder().metadata(Metadata.builder().name("connect1").build()).build(),
                Connector.builder().metadata(Metadata.builder().name("connect2").build()).build()));

        List<String> validationErrors = resourceQuotaService.validateConnectorQuota(ns);
        assertEquals(0, validationErrors.size());
    }

    @Test
    void validateConnectorQuotaNoQuota() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
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
        assertEquals(0, validationErrors.size());
    }

    @Test
    void validateConnectorQuotaExceed() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_CONNECTORS.toString(), "2"))
            .build();

        when(resourceQuotaRepository.findForNamespace("namespace"))
            .thenReturn(Optional.of(resourceQuota));
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of(
                Connector.builder().metadata(Metadata.builder().name("connect1").build()).build(),
                Connector.builder().metadata(Metadata.builder().name("connect2").build()).build()));

        List<String> validationErrors = resourceQuotaService.validateConnectorQuota(ns);
        assertEquals(1, validationErrors.size());
        assertEquals("Invalid \"apply\" operation: exceeding quota for count/connectors: 2/2 (used/limit).",
            validationErrors.get(0));
    }

    @Test
    void getUsedResourcesByQuotaByNamespace() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "3",
                COUNT_PARTITIONS.toString(), "20",
                COUNT_CONNECTORS.toString(), "2",
                DISK_TOPICS.toString(), "60KiB"))
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of("retention.bytes", "1000"))
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .configs(Map.of("retention.bytes", "2000"))
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .configs(Map.of("retention.bytes", "4000"))
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of(
                Connector.builder().metadata(Metadata.builder().name("connect1").build()).build(),
                Connector.builder().metadata(Metadata.builder().name("connect2").build()).build()));

        ResourceQuotaResponse response =
            resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns, Optional.of(resourceQuota));
        assertEquals(resourceQuota.getMetadata(), response.getMetadata());
        assertEquals("3/3", response.getSpec().getCountTopic());
        assertEquals("19/20", response.getSpec().getCountPartition());
        assertEquals("2/2", response.getSpec().getCountConnector());
        assertEquals("50.782KiB/60KiB", response.getSpec().getDiskTopic());
    }

    @Test
    void getCurrentResourcesQuotasByNamespaceNoQuota() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of("retention.bytes", "1000"))
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .configs(Map.of("retention.bytes", "2000"))
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .configs(Map.of("retention.bytes", "4000"))
                .build())
            .build();

        when(topicService.findAllForNamespace(ns))
            .thenReturn(List.of(topic1, topic2, topic3));
        when(connectorService.findAllForNamespace(ns))
            .thenReturn(List.of(
                Connector.builder().metadata(Metadata.builder().name("connect1").build()).build(),
                Connector.builder().metadata(Metadata.builder().name("connect2").build()).build()));

        ResourceQuotaResponse response = resourceQuotaService.getUsedResourcesByQuotaByNamespace(ns, Optional.empty());
        assertEquals("namespace", response.getMetadata().getNamespace());
        assertEquals("local", response.getMetadata().getCluster());
        assertEquals("3", response.getSpec().getCountTopic());
        assertEquals("19", response.getSpec().getCountPartition());
        assertEquals("2", response.getSpec().getCountConnector());
        assertEquals("50.782KiB", response.getSpec().getDiskTopic());
    }

    @Test
    void getUsedQuotaByNamespaces() {
        Namespace ns1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Namespace ns2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace2")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Namespace ns3 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace3")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        Namespace ns4 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("namespace4")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .connectClusters(List.of("local-name"))
                .build())
            .build();

        ResourceQuota resourceQuota = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .cluster("local")
                .name("test")
                .build())
            .spec(Map.of(COUNT_TOPICS.toString(), "3",
                COUNT_PARTITIONS.toString(), "20",
                COUNT_CONNECTORS.toString(), "2",
                DISK_TOPICS.toString(), "60KiB"))
            .build();

        Topic topic1 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(6)
                .configs(Map.of("retention.bytes", "1000"))
                .build())
            .build();

        Topic topic2 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace2")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(3)
                .configs(Map.of("retention.bytes", "2000"))
                .build())
            .build();

        Topic topic3 = Topic.builder()
            .metadata(Metadata.builder()
                .name("topic")
                .namespace("namespace3")
                .build())
            .spec(Topic.TopicSpec.builder()
                .partitions(10)
                .configs(Map.of("retention.bytes", "4000"))
                .build())
            .build();

        when(topicService.findAllForNamespace(ns1))
            .thenReturn(List.of(topic1));
        when(topicService.findAllForNamespace(ns2))
            .thenReturn(List.of(topic2));
        when(topicService.findAllForNamespace(ns3))
            .thenReturn(List.of(topic3));
        when(topicService.findAllForNamespace(ns4))
            .thenReturn(List.of());
        when(connectorService.findAllForNamespace(any()))
            .thenReturn(List.of(
                Connector.builder().metadata(Metadata.builder().name("connect1").build()).build(),
                Connector.builder().metadata(Metadata.builder().name("connect2").build()).build()));
        when(resourceQuotaRepository.findForNamespace(any()))
            .thenReturn(Optional.of(resourceQuota));

        List<ResourceQuotaResponse> response =
            resourceQuotaService.getUsedQuotaByNamespaces(List.of(ns1, ns2, ns3, ns4));
        assertEquals(4, response.size());
    }
}
