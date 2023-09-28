package com.michelin.ns4kafka.models.consumer.group;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

@Introspected
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupResetOffsetsResponse {
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Resource kind
     */
    private final String kind = "ConsumerGroupResetOffsetsResponse";

    /**
     * Resource metadata
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * Resource specifications
     */
    @Valid
    @NotNull
    private ConsumerGroupResetOffsetsResponseSpec spec;

    @Getter
    @Builder
    @ToString
    @Introspected
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ConsumerGroupResetOffsetsResponseSpec {
        /**
         * The topic that was reset
         */
        private String topic;

        /**
         * The partition that was reset
         */
        private int partition;

        /**
         * The new offset
         */
        private Long offset;

        /**
         * The consumer group
         */
        private String consumerGroup;
    }
}
