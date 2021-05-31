package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;

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
    ConsumerGroupResetOffsetSpec spec;

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
        TO_EARLIEST, TO_DATE_TIME
    }


}
