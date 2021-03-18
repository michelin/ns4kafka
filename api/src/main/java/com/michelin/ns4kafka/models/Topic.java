package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Topic {
    private final String apiVersion = "v1";
    private final String kind = "Topic";
    @Valid
    @NotNull
    private ObjectMeta metadata;

    @NotNull
    private TopicSpec spec;

    @Schema(accessMode = Schema.AccessMode.READ_ONLY)
    private TopicStatus status;

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Schema(description = "Contains the Topic specification")
    public static class TopicSpec {
        @Schema(description = "Replication Factor",required = true, example = "3")
        private int replicationFactor;
        @Schema(description = "Number of partitions",required = true, example = "3")
        private int partitions;
        @Schema(description = "Topic configs",required = true, example = "cleanup.policy=delete")
        private Map<String,String> configs;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Schema(description = "Server-side",accessMode = Schema.AccessMode.READ_ONLY)
    public static class TopicStatus {
        private TopicPhase phase;
        private String message;
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

        public static TopicStatus ofSuccess(String message){
            return TopicStatus.builder()
                    .phase(TopicPhase.Success)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }
        public static TopicStatus ofFailed(String message){
            return TopicStatus.builder()
                    .phase(TopicPhase.Failed)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }
        public static TopicStatus ofPending(){
            return Topic.TopicStatus.builder()
                    .phase(Topic.TopicPhase.Pending)
                    .message("Awaiting processing by executor")
                    .build();
        }
    }
    public enum TopicPhase {
        Pending,
        Success,
        Failed
    }

}
