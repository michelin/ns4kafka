package com.michelin.ns4kafka.models.connector;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Change connector state.
 */
@Data
@Builder
@Introspected
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

    /**
     * Connector action.
     */
    public enum ConnectorAction {
        pause,
        resume,
        restart
    }

    /**
     * Change connector state specification.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChangeConnectorStateSpec {
        @NotNull
        private ConnectorAction action;
    }

    /**
     * Change connector state status.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChangeConnectorStateStatus {
        private boolean success;
        private HttpStatus code;
        private String errorMessage;
    }
}
