package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Subject {
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Kind of resource
     */
    private final String kind = "Subject";

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
    private SubjectSpec spec;

    /**
     * Current status of the schema
     */
    @EqualsAndHashCode.Exclude
    private Subject.SubjectStatus status;

    /**
     * Schema specifications
     */
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class SubjectSpec {
        /**
         * Schema compatibility
         */
        private SubjectCompatibility compatibility;

        /**
         * Reference on the file containing the schema to load
         */
        private String schemaReference;

        /**
         * Content of the schema
         */
        private Subject.SubjectSpec.Content schemaContent;

        /**
         * Schema specifications
         */
        @Builder
        @AllArgsConstructor
        @NoArgsConstructor
        @Data
        @io.swagger.v3.oas.annotations.media.Schema(description = "Server-side",
                accessMode = io.swagger.v3.oas.annotations.media.Schema.AccessMode.READ_ONLY)
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
    public static class SubjectStatus {
        private Subject.SubjectPhase phase;
        private String message;
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Date lastUpdateTime;

        public static Subject.SubjectStatus ofSuccess(String message) {
            return Subject.SubjectStatus.builder()
                    .phase(Subject.SubjectPhase.Success)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static Subject.SubjectStatus ofFailed(String message) {
            return Subject.SubjectStatus.builder()
                    .phase(Subject.SubjectPhase.Failed)
                    .message(message)
                    .lastUpdateTime(Date.from(Instant.now()))
                    .build();
        }

        public static Subject.SubjectStatus ofPending() {
            return Subject.SubjectStatus.builder()
                    .phase(Subject.SubjectPhase.Pending)
                    .message("Awaiting processing by executor")
                    .build();
        }
    }

    /**
     * Phases of a schema
     */
    public enum SubjectPhase {
        // Pending when pushed in the technical schema topic, but not published to schema registry yet
        Pending,
        // Published to the schema registry
        Success,
        // Publishing failed
        Failed
    }

    /**
     * Subject compatibility for Schema Registry
     */
    public enum SubjectCompatibility {
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE,
        NONE
    }
}
