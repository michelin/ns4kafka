package com.michelin.ns4kafka.models;

import io.micronaut.serde.annotation.Serdeable;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

@Data
@Builder
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class Topic {
    private final String apiVersion = "v1";
    private final String kind = "Topic";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @NotNull
    private TopicSpec spec;

    @EqualsAndHashCode.Exclude
    private TopicStatus status;

    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopicSpec {
        private int replicationFactor;
        private int partitions;
        private Map<String, String> configs;
    }

    @Getter
    @Setter
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    @Schema(description = "Server-side", accessMode = Schema.AccessMode.READ_ONLY)
    public static class TopicStatus {
        private TopicPhase phase;
        private String message;
        private Date lastUpdateTime;

        /**
         * Success status
         * @param message A success message
         * @return A success topic status
         */
        public static TopicStatus ofSuccess(String message) {
            return TopicStatus.builder()
                    .phase(TopicPhase.Success)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        /**
         * Failed status
         * @param message A failure message
         * @return A failure topic status
         */
        public static TopicStatus ofFailed(String message) {
            return TopicStatus.builder()
                    .phase(TopicPhase.Failed)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        /**
         * Pending status
         * @return A pending topic status
         */
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
