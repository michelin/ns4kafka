package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.common.TopicPartition;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ConsumerGroupResetOffset {

    private final String apiVersion = "v1";
    private final String kind = "ConsumerGroupResetOffset";
    @Valid
    @NotNull
    private ObjectMeta metadata;
    @Valid
    @NotNull
    private ConsumerGroupResetOffsetSpec spec;
    @Valid
    private ConsumerGroupResetOffsetStatus status;


    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class ConsumerGroupResetOffsetSpec {
        @NotNull
        private String topic;
        @NotNull
        private ConsumerGroupResetOffsetMethod method;
        private Long timestamp;
        private AlterConsumerGroupOffsetsOptions option;
    }

    @Introspected
    public static enum ConsumerGroupResetOffsetMethod {
        TO_EARLIEST, TO_DATETIME
    }

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class ConsumerGroupResetOffsetStatus {
        private boolean success;
        private Map<String, Long> offsetChanged;

    }

}
