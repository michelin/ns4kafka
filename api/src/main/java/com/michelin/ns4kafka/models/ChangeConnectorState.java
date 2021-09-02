package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ChangeConnectorState {
    private final String apiVersion = "v1";
    private final String kind = "ChangeConnectorState";
    @Valid
    @NotNull
    private ObjectMeta metadata;
    @Valid
    @NotNull
    private ChangeConnectorStateSpec spec;
    private ChangeConnectorStateStatus status;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class ChangeConnectorStateSpec {
        @NotNull
        private ConnectorAction action;
        //TODO
        // connectCluster
        // taskId
    }

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class ChangeConnectorStateStatus {
        private boolean success;
        private HttpStatus code;
        private String errorMessage;
    }

    public enum ConnectorAction {
        pause,
        resume,
        restart
    }
}
