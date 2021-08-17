package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Map;

@Introspected
@Data
@EqualsAndHashCode(callSuper=false)
public class ConsumerGroupResetOffsets extends Resource {

    @Builder
    public ConsumerGroupResetOffsets(@NotNull ObjectMeta metadata, ConsumerGroupResetOffsetsSpec spec) {
        super("v1","ConsumerGroupResetOffsets", metadata);
        this.spec = spec;
    }
    @Valid
    @NotNull
    private ConsumerGroupResetOffsetsSpec spec;
    private ConsumerGroupResetOffsetStatus status;


    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
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
        TO_DATETIME,
        BY_DURATION,
        SHIFT_BY
        //FROM_FILE
    }
    // TO_EARLIEST      {}
    // TO_LATEST        {}
    // TO_DATETIME      {string:yyyy-MM-ddTHH:mm:SS.sss}
    // SHIFT_BY         {int}
    // FROM_FILE        {map<string:topic-partition,long:offset}
    // BY_DURATION      {

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class ConsumerGroupResetOffsetStatus {
        private boolean success;
        private String errorMessage;
        private Map<String, Long> offsetChanged;

        public static ConsumerGroupResetOffsetStatus ofSuccess(Map<String, Long> offsetChanged) {
            return ConsumerGroupResetOffsetStatus.builder()
                    .success(true)
                    .offsetChanged(offsetChanged)
                    .build();
        }

        public static ConsumerGroupResetOffsetStatus ofFailure(String errorMessage) {
            return ConsumerGroupResetOffsetStatus.builder()
                    .success(false)
                    .errorMessage(errorMessage)
                    .build();
        }
    }

}
