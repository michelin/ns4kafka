package com.michelin.ns4kafka.models.consumer.group;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

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

    @Getter
    @Setter
    @Builder
    @ToString
    @Introspected
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

    public enum ResetOffsetsMethod {
        TO_EARLIEST,
        TO_LATEST,
        TO_DATETIME, // string:yyyy-MM-ddTHH:mm:SS.sss
        BY_DURATION,
        SHIFT_BY,
        TO_OFFSET
    }
}
