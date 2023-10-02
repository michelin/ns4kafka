package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Delete records response.
 */
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

    /**
     * Delete records response specification.
     */
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
