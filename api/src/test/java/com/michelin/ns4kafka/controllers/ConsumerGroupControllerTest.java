package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetsSpec;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.ConsumerGroupService;
import com.michelin.ns4kafka.services.NamespaceService;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.security.utils.SecurityService;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ConsumerGroupControllerTest {

    @Mock
    NamespaceService namespaceService;
    @Mock
    ConsumerGroupService consumerGroupService;
    @Mock
    ApplicationEventPublisher applicationEventPublisher;
    @Mock
    SecurityService securityService;
    @InjectMocks
    ConsumerGroupController consumerGroupController;

    @Test
    void reset_Valid() throws InterruptedException, ExecutionException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(ObjectMeta.builder()
                        .name("groupID")
                        .cluster("local")
                        .build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .options(null)
                        .build())
                .build();
        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        List<TopicPartition> topicPartitions = List.of(topicPartition1, topicPartition2);

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset))
                .thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);
        when(consumerGroupService.getConsumerGroupStatus(ns, "groupID"))
                .thenReturn("Empty");
        when(consumerGroupService.getPartitionsToReset(ns, "groupID", "topic1"))
                .thenReturn(topicPartitions);
        when(consumerGroupService.prepareOffsetsToReset(ns, "groupID", null, topicPartitions, ResetOffsetsMethod.TO_EARLIEST))
                .thenReturn(Map.of(topicPartition1, 5L, topicPartition2, 10L));
        when(securityService.username()).thenReturn(Optional.of("test-user"));
        when(securityService.hasRole("Admin")).thenReturn(false);
        doNothing().when(applicationEventPublisher).publishEvent(any());

        ConsumerGroupResetOffsets result = consumerGroupController.resetOffsets("test", "groupID", resetOffset, false);

        assertTrue(result.getStatus().isSuccess());
        assertEquals(5L, result.getStatus().getOffsetChanged().get(topicPartition1.toString()));
        assertEquals(10L, result.getStatus().getOffsetChanged().get(topicPartition2.toString()));
        verify(consumerGroupService, times(1)).alterConsumerGroupOffsets(ArgumentMatchers.eq(ns), ArgumentMatchers.eq("groupID"), anyMap());
    }

    @Test
    void reset_DryRunSucces() throws InterruptedException, ExecutionException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(ObjectMeta.builder()
                        .name("groupID")
                        .cluster("local")
                        .build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();
        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        List<TopicPartition> topicPartitions = List.of(topicPartition1, topicPartition2);

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset))
                .thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);
        when(consumerGroupService.getConsumerGroupStatus(ns, "groupID"))
                .thenReturn("Empty");
        when(consumerGroupService.getPartitionsToReset(ns, "groupID", "topic1"))
                .thenReturn(topicPartitions);
        when(consumerGroupService.prepareOffsetsToReset(ns, "groupID", null, topicPartitions, ResetOffsetsMethod.TO_EARLIEST))
                .thenReturn(Map.of(topicPartition1, 5L, topicPartition2, 10L));

        ConsumerGroupResetOffsets result = consumerGroupController.resetOffsets("test", "groupID", resetOffset, true);

        assertTrue(result.getStatus().isSuccess());
        assertEquals(5L, result.getStatus().getOffsetChanged().get(topicPartition1.toString()));
        assertEquals(10L, result.getStatus().getOffsetChanged().get(topicPartition2.toString()));
        verify(consumerGroupService, never()).alterConsumerGroupOffsets(notNull(), anyString(), anyMap());
    }

    @Test
    void reset_ExecutionError() throws InterruptedException, ExecutionException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(ObjectMeta.builder()
                        .name("groupID")
                        .cluster("local")
                        .build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset))
                .thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);
        when(consumerGroupService.getConsumerGroupStatus(ns, "groupID"))
                .thenReturn("Empty");
        when(consumerGroupService.getPartitionsToReset(ns, "groupID", "topic1"))
                .thenThrow(new ExecutionException("Error", new Throwable()));

        ConsumerGroupResetOffsets result = consumerGroupController.resetOffsets("test", "groupID", resetOffset, false);

        assertFalse(result.getStatus().isSuccess());
        assertEquals("Error", result.getStatus().getErrorMessage());
    }

    @Test
    void reset_ValidationErrorNotOwnerOfConsumerGroup() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(ObjectMeta.builder()
                        .name("groupID")
                        .cluster("local")
                        .build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset))
                .thenReturn(new ArrayList<>());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(false);

        ResourceValidationException result = assertThrows(ResourceValidationException.class,
                () -> consumerGroupController.resetOffsets("test", "groupID", resetOffset, false));

        assertLinesMatch(List.of("Invalid value groupID for name: Namespace not OWNER of this consumer group"), result.getValidationErrors());
    }

    @Test
    void reset_ValidationErrorInvalidResource() {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(ObjectMeta.builder()
                        .name("groupID")
                        .cluster("local")
                        .build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset))
                .thenReturn(List.of("Validation Error"));
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);

        ResourceValidationException result = assertThrows(ResourceValidationException.class,
                () -> consumerGroupController.resetOffsets("test", "groupID", resetOffset, false));

        assertLinesMatch(List.of("Validation Error"), result.getValidationErrors());
    }

    @Test
    void reset_ValidationErrorConsumerGroupActive() throws ExecutionException, InterruptedException {
        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        ConsumerGroupResetOffsets resetOffset = ConsumerGroupResetOffsets.builder()
                .metadata(ObjectMeta.builder()
                        .name("groupID")
                        .cluster("local")
                        .build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("topic1")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        when(namespaceService.findByName("test"))
                .thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset))
                .thenReturn(new ArrayList<>());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup("test", "groupID"))
                .thenReturn(true);
        when(consumerGroupService.getConsumerGroupStatus(ns, "groupID"))
                .thenReturn("Active");

        ConsumerGroupResetOffsets result = consumerGroupController.resetOffsets("test", "groupID", resetOffset, false);

        assertFalse(result.getStatus().isSuccess());
        assertEquals("Assignments can only be reset if the group 'groupID' is inactive, but the current state is Active.",
                result.getStatus().getErrorMessage());
    }

}
