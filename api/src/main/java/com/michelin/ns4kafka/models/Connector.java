package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Introspected
@Data
@EqualsAndHashCode(callSuper = true)
public class Connector extends Resource {

    @Builder
    public Connector(@NotNull ObjectMeta metadata, ConnectorSpec spec, ConnectorStatus status) {
        super("v1","Connector", metadata);
        this.spec = spec;
        this.status = status;
    }

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
