package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

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
        private String topic;
        private ConsumerGroupResetOffsetMethod method;
        private Object option;
    }

    @Introspected
    public static enum ConsumerGroupResetOffsetMethod {
        TO_EARLIEST, TO_DATE_TIME
    }


}
