package com.michelin.ns4kafka.models.consumer.group;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Consumer group reset offsets.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupResetOffsets {
    private final String apiVersion = "v1";
    private final String kind = "ConsumerGroupResetOffsets";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private ConsumerGroupResetOffsetsSpec spec;

    /**
     * Represents the reset offsets method.
     */
    public enum ResetOffsetsMethod {
        TO_EARLIEST,
        TO_LATEST,
        TO_DATETIME, // string:yyyy-MM-ddTHH:mm:SS.sss
        BY_DURATION,
        SHIFT_BY,
        TO_OFFSET
    }

    /**
     * Consumer group reset offsets specification.
     */
    @Getter
    @Setter
    @Builder
    @ToString
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConsumerGroupResetOffsetsSpec {
        @NotNull
        @NotBlank
        private String topic;

        @NotNull
        private ResetOffsetsMethod method;
        private String options;
    }
}
