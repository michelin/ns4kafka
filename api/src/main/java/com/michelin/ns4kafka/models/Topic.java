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
public class Topic {
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Kind of resource
     */
    private final String kind = "Topic";

    /**
     * Schema metadata
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * Topic specifications
     */
    @NotNull
    private TopicSpec spec;

    /**
     * Topic status
     */
    @EqualsAndHashCode.Exclude
    private TopicStatus status;

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class TopicSpec {
        /**
         * Replication factor
         */
        private int replicationFactor;

        /**
         * Partitions quantity
         */
        private int partitions;

        /**
         * Topic configuration
         */
        private Map<String, String> configs;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Schema(description = "Server-side", accessMode = Schema.AccessMode.READ_ONLY)
    public static class TopicStatus {
        /**
         * Topic phase
         */
        private TopicPhase phase;

        /**
         * Message
         */
        private String message;

        /**
         * Last updated time
         */
        @JsonFormat(shape = JsonFormat.Shape.STRING)
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
