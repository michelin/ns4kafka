package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import java.util.Map;

@Introspected
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class DeleteRecords {
    private final String apiVersion = "v1";
    private final String kind = "DeleteRecords";

    private ObjectMeta metadata;
    private DeleteRecordsStatus status;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class DeleteRecordsStatus {
        private boolean success;
        private String errorMessage;
        private Map<String, Long> lowWaterMarks;

    }
}
