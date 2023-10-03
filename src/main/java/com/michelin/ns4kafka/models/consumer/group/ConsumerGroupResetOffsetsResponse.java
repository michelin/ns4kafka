package com.michelin.ns4kafka.models.consumer.group;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Consumer group reset offsets response.
 */
@Getter
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupResetOffsetsResponse {
    private final String apiVersion = "v1";
    private final String kind = "ConsumerGroupResetOffsetsResponse";

    /**
     * Resource metadata.
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * Resource specifications.
     */
    @Valid
    @NotNull
    private ConsumerGroupResetOffsetsResponseSpec spec;

    /**
     * Consumer group reset offsets response specification.
     */
    @Getter
    @Builder
    @ToString
    @Introspected
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ConsumerGroupResetOffsetsResponseSpec {
        /**
         * The topic that was reset.
         */
        private String topic;

        /**
         * The partition that was reset.
         */
        private int partition;

        /**
         * The new offset.
         */
        private Long offset;

        /**
         * The consumer group.
         */
        private String consumerGroup;
    }
}
