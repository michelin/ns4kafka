package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ConsumerGroup {

    private final String apiVersion = "v1";
    private final String kind = "ConsumerGroup";
    @Valid
    @NotNull
    private ObjectMeta metadata;
    // @Valid
    // @NotNull
    // ConsumerGroupSpec spec;

    // @Introspected
    // @Builder
    // @AllArgsConstructor
    // @NoArgsConstructor
    // @Getter
    // @Setter
    // @ToString
    // public static class ConsumerGroupSpec {


    // }

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class ConsumerOffset {
        private String topic;
        private List<PartitionOffset> partitionOffsets;
    }

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class PartitionOffset {
        private int partition;
        private long offset;
    }
}
