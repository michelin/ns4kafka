package com.michelin.ns4kafka.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.consumer.group.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.consumer.group.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetsSpec;
import com.michelin.ns4kafka.models.consumer.group.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.services.executors.ConsumerGroupAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConsumerGroupServiceTest {
    @Mock
    ApplicationContext applicationContext;

    @InjectMocks
    ConsumerGroupService consumerGroupService;

    @Test
    void doValidationAllTopics() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("*")
                .method(ResetOffsetsMethod.TO_EARLIEST)
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationAllPartitionsFromTopic() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01")
                .method(ResetOffsetsMethod.TO_EARLIEST)
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationSpecificPartitionFromTopic() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_EARLIEST)
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationMissingTopic() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic(":2")
                .method(ResetOffsetsMethod.TO_EARLIEST)
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @Test
    void doValidationEarliestOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_EARLIEST)
                .options(null)
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationLatestOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_LATEST)
                .options(null)
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationValidDateTimeOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_DATETIME)
                .options("2021-06-02T11:23:33.249+02:00")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationValidDateTimeOptionDateWithoutMs() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_DATETIME)
                .options("2021-06-02T11:22:33+02:00")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationInvalidDateTimeOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_DATETIME)
                .options("NOT A DATE")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @Test
    void doValidationInvalidDateTimeOptionDateWithoutTz() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_DATETIME)
                .options("2021-06-02T11:22:33.249")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @Test
    void doValidationInvalidDateTimeOptionDateWithInvalidTz() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_DATETIME)
                .options("2021-06-02T11:22:33+99:99")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @Test
    void doValidationValidMinusShiftByOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.SHIFT_BY)
                .options("-5")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationValidPositiveShiftByOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.SHIFT_BY)
                .options("+5")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidation_InvalidShiftByOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.SHIFT_BY)
                .options("Not an integer")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @Test
    void doValidationValidPositiveDurationOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.BY_DURATION)
                .options("P4DT11H9M8S")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidationValidMinusDurationOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.BY_DURATION)
                .options("-P4DT11H9M8S")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidation_InvalidDurationOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.BY_DURATION)
                .options("P4T11H9M8S")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals(1, result.size());
    }

    @Test
    void shouldValidateOffsetFormatInt() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_OFFSET)
                .options("not-integer")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals("Invalid value \"not-integer\" for field \"to-offset\": value must be an integer.", result.get(0));
    }

    @Test
    void shouldValidateOffsetFormatPositive() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_OFFSET)
                .options("-1")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertEquals("Invalid value \"-1\" for field \"to-offset\": value must be >= 0.", result.get(0));
    }

    @Test
    void doGetPartitionsToResetAllTopic() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .cluster("test")
                .build())
            .build();
        String groupId = "testGroup";
        String topic = "*";

        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        TopicPartition topicPartition3 = new TopicPartition("topic2", 0);

        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = mock(ConsumerGroupAsyncExecutor.class);
        when(applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
            Qualifiers.byName(namespace.getMetadata().getCluster()))).thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.getCommittedOffsets(groupId)).thenReturn(
            Map.of(topicPartition1, 5L,
                topicPartition2, 10L,
                topicPartition3, 5L));
        List<TopicPartition> result = consumerGroupService.getPartitionsToReset(namespace, groupId, topic);

        assertEquals(3, result.size());

        List<TopicPartition> partitionsToReset = List.of(topicPartition1, topicPartition2, topicPartition3);
        assertEquals(new HashSet<>(partitionsToReset), new HashSet<>(result));
    }

    @Test
    void doGetPartitionsToResetOneTopic() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .cluster("test")
                .build())
            .build();
        String groupId = "testGroup";
        String topic = "topic1";

        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);

        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = mock(ConsumerGroupAsyncExecutor.class);
        when(applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
            Qualifiers.byName(namespace.getMetadata().getCluster()))).thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.getTopicPartitions(topic)).thenReturn(
            List.of(topicPartition1, topicPartition2));
        List<TopicPartition> result = consumerGroupService.getPartitionsToReset(namespace, groupId, topic);

        assertEquals(2, result.size());
        assertEquals(new HashSet<>(List.of(topicPartition1, topicPartition2)), new HashSet<>(result));
    }

    @Test
    void doGetPartitionsToResetOneTopicPartition() throws InterruptedException, ExecutionException {
        Namespace namespace = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .cluster("test")
                .build())
            .build();
        String groupId = "testGroup";
        String topic = "topic1:0";

        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);

        List<TopicPartition> result = consumerGroupService.getPartitionsToReset(namespace, groupId, topic);

        assertEquals(1, result.size());
        assertEquals(List.of(topicPartition1), result);
    }

    @Test
    void doPrepareOffsetsToResetShiftBy() throws ExecutionException, InterruptedException {
        Namespace namespace = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .cluster("test")
                .build())
            .build();
        String groupId = "testGroup";
        String options = "-5";
        TopicPartition topicPartition1 = new TopicPartition("topic1", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic1", 1);
        List<TopicPartition> partitionsToReset = List.of(topicPartition1, topicPartition2);

        ResetOffsetsMethod method = ResetOffsetsMethod.SHIFT_BY;
        ConsumerGroupAsyncExecutor consumerGroupAsyncExecutor = mock(ConsumerGroupAsyncExecutor.class);
        when(applicationContext.getBean(ConsumerGroupAsyncExecutor.class,
            Qualifiers.byName(namespace.getMetadata().getCluster()))).thenReturn(consumerGroupAsyncExecutor);
        when(consumerGroupAsyncExecutor.getCommittedOffsets(anyString())).thenReturn(
            Map.of(new TopicPartition("topic1", 0), 10L,
                new TopicPartition("topic1", 1), 15L,
                new TopicPartition("topic2", 0), 10L));
        when(consumerGroupAsyncExecutor.checkOffsetsRange(
            Map.of(new TopicPartition("topic1", 0), 5L,
                new TopicPartition("topic1", 1), 10L))).thenReturn(
            Map.of(new TopicPartition("topic1", 0), 5L,
                new TopicPartition("topic1", 1), 10L));

        Map<TopicPartition, Long> result =
            consumerGroupService.prepareOffsetsToReset(namespace, groupId, options, partitionsToReset, method);

        assertEquals(2, result.size());
        assertTrue(result.containsKey(topicPartition1));
        assertTrue(result.containsKey(topicPartition2));
        assertEquals(5, result.get(topicPartition1));
        assertEquals(10, result.get(topicPartition2));
    }
}
