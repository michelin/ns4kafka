package com.michelin.ns4kafka.services;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetsSpec;
import com.michelin.ns4kafka.models.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConsumerGroupServiceTest {
    @Mock
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfigs;

    @InjectMocks
    ConsumerGroupService consumerGroupService;

    @Test
    void doValidation_AllTopics() {
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
    void doValidation_AllPartitionsFromTopic() {
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
    void doValidation_SpecificPartitionFromTopic() {
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
    void doValidation_EarliestOption() {
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
    void doValidation_LatestOption() {
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
    void doValidation_ValidDateTimeOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_DATETIME)
                .options("2021-06-02T11:23:33.0249+02:00")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.isEmpty());
    }

    @Test
    void doValidation_InvalidDateTimeOption() {
        ConsumerGroupResetOffsets consumerGroupResetOffsets = ConsumerGroupResetOffsets.builder()
            .spec(ConsumerGroupResetOffsetsSpec.builder()
                .topic("namespace_testTopic01:2")
                .method(ResetOffsetsMethod.TO_DATETIME)
                .options("2021-06-025T11:23:33.0249+02:00")
                .build())
            .build();

        List<String> result = consumerGroupService.validateResetOffsets(consumerGroupResetOffsets);
        assertTrue(result.size() == 1);
    }

    @Test
    void doValidation_ValidMinusShiftByOption() {
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
    void doValidation_ValidPositiveShiftByOption() {
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
        assertTrue(result.size() == 1);
    }

    @Test
    void doValidation_ValidPositiveDurationOption() {
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
    void doValidation_ValidMinusDurationOption() {
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
        assertTrue(result.size() == 1);
    }
}
