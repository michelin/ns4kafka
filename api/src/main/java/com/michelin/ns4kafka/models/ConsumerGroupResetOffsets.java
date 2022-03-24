package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Map;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ConsumerGroupResetOffsets {
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Resource kind
     */
    private final String kind = "ConsumerGroupResetOffsets";

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
    private ConsumerGroupResetOffsetsSpec spec;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class ConsumerGroupResetOffsetsSpec {
        /**
         * The topic to reset offsets
         */
        @NotNull
        @NotBlank
        private String topic;

        /**
         * The method used to reset offsets
         */
        @NotNull
        private ResetOffsetsMethod method;

        /**
         * Additional options for offsets reset
         */
        private String options;
    }

    /**
     * All reset offsets method
     */
    @Introspected
    public enum ResetOffsetsMethod {
        TO_EARLIEST,
        TO_LATEST,
        TO_DATETIME, // string:yyyy-MM-ddTHH:mm:SS.sss
        BY_DURATION,
        SHIFT_BY, // int
        TO_OFFSET
        // FROM_FILE map<string:topic-partition,long:offset
    }
}
