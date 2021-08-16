package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Topic extends Resource{
    private final String apiVersion = "v1";
    private final String kind = "Topic";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @NotNull
    private TopicSpec spec;

    @EqualsAndHashCode.Exclude
    private TopicStatus status;

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class TopicSpec {
        private int replicationFactor;
        private int partitions;
        private Map<String, String> configs;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Schema(description = "Server-side", accessMode = Schema.AccessMode.READ_ONLY)
    public static class TopicStatus {
        private TopicPhase phase;
        private String message;
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

        public static TopicStatus ofSuccess(String message) {
            return TopicStatus.builder()
                    .phase(TopicPhase.Success)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static TopicStatus ofFailed(String message) {
            return TopicStatus.builder()
                    .phase(TopicPhase.Failed)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static TopicStatus ofPending() {
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
