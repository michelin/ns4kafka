package com.michelin.ns4kafka.models.connector;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class ConnectorSpec {
        @NotBlank
        private String connectCluster;

        @NotNull
        @JsonInclude(value = JsonInclude.Include.NON_ABSENT)
        private Map<String, String> config;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class ConnectorStatus {
        private TaskState state;
        private String worker_id;
        private List<TaskStatus> tasks;

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
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
