package com.michelin.ns4kafka.models.connector;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.http.HttpStatus;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
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

    @Data
    @Builder
    @Serdeable
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ChangeConnectorStateSpec {
        @NotNull
        private ConnectorAction action;
    }

    @Data
    @Builder
    @Serdeable
    @AllArgsConstructor
    @NoArgsConstructor
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
