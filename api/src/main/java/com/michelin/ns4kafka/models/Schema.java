package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Date;

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
         * AVSC schema
         */
        private String schema;
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
        Failed
    }
}
