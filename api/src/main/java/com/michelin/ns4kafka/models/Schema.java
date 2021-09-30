package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Schema {
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Kind of resource
     */
    private final String kind = "Schema";

    /**
     * Resource metadata
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * The schema specifications
     */
    @Valid
    @NotNull
    private SchemaSpec spec;

    /**
     * Current status of the schema
     */
    @EqualsAndHashCode.Exclude
    private Schema.SchemaStatus status;

    /**
     * Schema specifications
     */
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class SchemaSpec {
        /**
         * Schema compatibility
         */
        private SchemaCompatibility compatibility;

        /**
         * Content of the schema
         */
        private Schema.SchemaSpec.Content content;

        /**
         * Schema specifications
         */
        @Builder
        @AllArgsConstructor
        @NoArgsConstructor
        @Data
        public static class Content {
            /**
             * AVSC schema
             */
            @NotNull
            private String schema;
        }
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @io.swagger.v3.oas.annotations.media.Schema(description = "Server-side",
            accessMode = io.swagger.v3.oas.annotations.media.Schema.AccessMode.READ_ONLY)
    public static class SchemaStatus {
        private Schema.SchemaPhase phase;
        private String message;
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

        public static Schema.SchemaStatus ofSuccess(String message) {
            return Schema.SchemaStatus.builder()
                    .phase(Schema.SchemaPhase.Success)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static Schema.SchemaStatus ofFailed(String message) {
            return Schema.SchemaStatus.builder()
                    .phase(Schema.SchemaPhase.Failed)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static Schema.SchemaStatus ofPending() {
            return Schema.SchemaStatus.builder()
                    .phase(Schema.SchemaPhase.Pending)
                    .message("Awaiting processing by executor")
                    .build();
        }

        public static Schema.SchemaStatus ofSoftDeleted() {
            return Schema.SchemaStatus.builder()
                    .phase(Schema.SchemaPhase.SoftDeleted)
                    .message("Soft deleted from Schema Registry")
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static Schema.SchemaStatus ofFailedSoftDeletion() {
            return Schema.SchemaStatus.builder()
                    .phase(SchemaPhase.FailedSoftDeletion)
                    .message("Soft deletion failed")
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static Schema.SchemaStatus ofPendingSoftDeletion() {
            return Schema.SchemaStatus.builder()
                    .phase(SchemaPhase.PendingSoftDeletion)
                    .message("Awaiting processing by executor for soft deletion")
                    .build();
        }

        public static Schema.SchemaStatus ofFailedHardDeletion() {
            return Schema.SchemaStatus.builder()
                    .phase(SchemaPhase.FailedHardDeletion)
                    .message("Hard deletion failed")
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static Schema.SchemaStatus ofPendingHardDeletion() {
            return Schema.SchemaStatus.builder()
                    .phase(SchemaPhase.PendingHardDeletion)
                    .message("Awaiting processing by executor for hard deletion")
                    .build();
        }

        /**
         * Is a schema in deletion or deleted ?
         *
         * @param status The status of the schema
         * @return true if in deletion/deleted, false otherwise
         */
        public static boolean inDeletion(SchemaPhase status) {
            return Arrays.asList(SchemaPhase.PendingSoftDeletion, SchemaPhase.FailedSoftDeletion, Schema.SchemaPhase.SoftDeleted,
                    SchemaPhase.PendingHardDeletion, SchemaPhase.FailedHardDeletion)
                    .contains(status);
        }
    }

    /**
     * Phases of a schema
     */
    public enum SchemaPhase {
        // Pending when pushed in the technical schema topic, but not published to schema registry yet
        Pending,
        // Published to the schema registry
        Success,
        // Publishing failed
        Failed,
        // Pending when soft deletion is asked, but not deleted from schema registry
        PendingSoftDeletion,
        // When soft deletion has been applied to the schema registry
        SoftDeleted,
        // Soft deletion failed
        FailedSoftDeletion,
        // Pending when hard deletion is asked, but not deleted from schema registry
        PendingHardDeletion,
        // Hard deletion failed
        FailedHardDeletion
    }

    /**
     * Schema compatibility for Schema Registry
     */
    public enum SchemaCompatibility {
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE,
        NONE
    }
}
