package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Introspected
@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TopicPartitionOffset {
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Resource kind
     */
    private final String kind = "TopicPartitionOffset";

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
    private TopicPartitionOffsetSpec spec;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @ToString
    public static class TopicPartitionOffsetSpec {
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
