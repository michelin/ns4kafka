package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

@Getter
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class DeleteRecordsResponse {
    private final String apiVersion = "v1";
    private final String kind = "DeleteRecordsResponse";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private DeleteRecordsResponseSpec spec;

    @Getter
    @Builder
    @ToString
    @Introspected
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DeleteRecordsResponseSpec {
        private String topic;
        private int partition;
        private Long offset;
    }
}
