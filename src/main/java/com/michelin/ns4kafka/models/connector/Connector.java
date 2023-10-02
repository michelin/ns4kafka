package com.michelin.ns4kafka.models.connector;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Connector.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class Connector {
    private final String apiVersion = "v1";
    private final String kind = "Connector";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private ConnectorSpec spec;

    @EqualsAndHashCode.Exclude
    private ConnectorStatus status;

    /**
     * Connector task state.
     */
    public enum TaskState {
        // From https://github.com/apache/kafka/blob/trunk/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/AbstractStatus.java
        UNASSIGNED,
        RUNNING,
        PAUSED,
        FAILED,
        DESTROYED,
    }

    /**
     * Connector specification.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectorSpec {
        @NotBlank
        private String connectCluster;

        @NotNull
        @JsonInclude(value = JsonInclude.Include.NON_ABSENT)
        private Map<String, String> config;
    }

    /**
     * Connector status.
     */
    @Getter
    @Setter
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectorStatus {
        private TaskState state;
        private String workerId;
        private List<TaskStatus> tasks;

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

    }

    /**
     * Connector task status.
     */
    @Getter
    @Setter
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskStatus {
        String id;
        TaskState state;
        String trace;
        String workerId;
    }
}
