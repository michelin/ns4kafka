package com.michelin.ns4kafka.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetsSpec;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.ConsumerGroupService;
import com.michelin.ns4kafka.services.NamespaceService;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsumerGroupControllerTest {

    @Mock
    NamespaceService namespaceService;
    @Mock
    ConsumerGroupService consumerGroupService;
    @InjectMocks
    ConsumerGroupController consumerGroupController;

    @Test
    void reset_Valid() throws InterruptedException, ExecutionException, ParseException {
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

        when(namespaceService.findByName(anyString())).thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset)).thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup(anyString(), anyString())).thenReturn(true);
        when(consumerGroupService.getPartitionsToReset(any(), anyString(), anyString())).thenReturn(List.of(topicPartition1, topicPartition2));
        when(consumerGroupService.prepareOffsetsToReset(any(), anyString(), anyString(), anyList(),any())).thenReturn(Map.of(topicPartition1, 5L,topicPartition2,10L));
        when(consumerGroupService.apply(any(), anyString(), anyMap())).thenReturn(Map.of(topicPartition1.toString(),5L,topicPartition2.toString(),10L));

        ConsumerGroupResetOffsets result = consumerGroupController.resetOffsets("test", "groupID", resetOffset, false);
        assertTrue(result.getStatus().isSuccess());
        assertEquals(result.getStatus().getOffsetChanged().get(topicPartition1.toString()),5L);
        assertEquals(result.getStatus().getOffsetChanged().get(topicPartition2.toString()), 10L);



    }

    @Test
    void reset_DryRun() throws ParseException, InterruptedException, ExecutionException {
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

        when(namespaceService.findByName(anyString())).thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(resetOffset)).thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup(anyString(), anyString())).thenReturn(true);
        when(consumerGroupService.getPartitionsToReset(any(), anyString(), anyString())).thenReturn(List.of(topicPartition1, topicPartition2));
        when(consumerGroupService.prepareOffsetsToReset(any(), anyString(), anyString(), anyList(),any())).thenReturn(Map.of(topicPartition1, 5L,topicPartition2,10L));
        when(consumerGroupService.apply(any(), anyString(), anyMap())).thenReturn(Map.of(topicPartition1.toString(),5L,topicPartition2.toString(),10L));

        ConsumerGroupResetOffsets result = consumerGroupController.resetOffsets("test", "groupID", resetOffset, true);
        assertTrue(result.getStatus().isSuccess());
        assertEquals(result.getStatus().getOffsetChanged().get(topicPartition1.toString()), 5L);
        assertEquals(result.getStatus().getOffsetChanged().get(topicPartition2.toString()), 10L);

    }

    @Test
    void reset_Error() throws InterruptedException, ExecutionException {
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

        when(namespaceService.findByName(anyString())).thenReturn(Optional.of(ns));
        when(consumerGroupService.validateResetOffsets(any())).thenReturn(List.of());
        when(consumerGroupService.isNamespaceOwnerOfConsumerGroup(anyString(), anyString())).thenReturn(true);
        when(consumerGroupService.getPartitionsToReset(any(), anyString(), anyString())).thenThrow(new ExecutionException("Error", new Throwable()));

        ConsumerGroupResetOffsets result = consumerGroupController.resetOffsets("test", "groupID", resetOffset, false);
        assertFalse(result.getStatus().isSuccess());
        assertFalse(result.getStatus().getErrorMessage().isBlank());


    }


}
