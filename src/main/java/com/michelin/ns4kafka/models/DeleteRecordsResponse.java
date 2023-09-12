package com.michelin.ns4kafka.models;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Builder
@Serdeable
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

    @Builder
    @Getter
    @ToString
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DeleteRecordsResponseSpec {
        private String topic;
        private int partition;
        private Long offset;
    }
}
