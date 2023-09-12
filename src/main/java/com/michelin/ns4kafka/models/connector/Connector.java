package com.michelin.ns4kafka.models.connector;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@Builder
@Serdeable
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

    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectorSpec {
        @NotBlank
        private String connectCluster;

        @NotNull
        @JsonInclude(value = JsonInclude.Include.NON_ABSENT)
        private Map<String, String> config;
    }

    @Getter
    @Setter
    @Builder
    @Serdeable
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ConnectorStatus {
        private TaskState state;
        private String worker_id;
        private List<TaskStatus> tasks;
        private Date lastUpdateTime;

    }

    @Getter
    @Setter
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskStatus {
        String id;
        TaskState state;
        String trace;
        String worker_id;
    }

    public enum TaskState {
        // From https://github.com/apache/kafka/blob/trunk/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/AbstractStatus.java
        UNASSIGNED,
        RUNNING,
        PAUSED,
        FAILED,
        DESTROYED,
    }
}
