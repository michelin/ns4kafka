package com.michelin.ns4kafka.models.connector;

import static com.michelin.ns4kafka.utils.enums.Kind.CHANGE_CONNECTOR_STATE;

import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Change connector state.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class ChangeConnectorState extends MetadataResource {
    @Valid
    @NotNull
    private ChangeConnectorStateSpec spec;
    private ChangeConnectorStateStatus status;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     * @param status   The status
     */
    @Builder
    public ChangeConnectorState(Metadata metadata, ChangeConnectorStateSpec spec,
                                ChangeConnectorStateStatus status) {
        super("v1", CHANGE_CONNECTOR_STATE, metadata);
        this.spec = spec;
        this.status = status;
    }

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
