package com.michelin.ns4kafka.models.consumer.group;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.serde.annotation.Serdeable;
import lombok.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Data
@Builder
@Serdeable
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

    @Getter
    @Setter
    @Builder
    @ToString
    @Serdeable
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ConsumerGroupResetOffsetsSpec {
        @NotNull
        @NotBlank
        private String topic;

        @NotNull
        private ResetOffsetsMethod method;

        private String options;
    }

    @Introspected
    public enum ResetOffsetsMethod {
        TO_EARLIEST,
        TO_LATEST,
        TO_DATETIME, // string:yyyy-MM-ddTHH:mm:SS.sss
        BY_DURATION,
        SHIFT_BY,
        TO_OFFSET
    }
}
