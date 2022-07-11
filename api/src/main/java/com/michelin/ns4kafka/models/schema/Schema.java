package com.michelin.ns4kafka.models.schema;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
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
     * Schema metadata
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * Schema specifications
     */
    @Valid
    @NotNull
    private SchemaSpec spec;

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class SchemaSpec {
        /**
         * Schema ID
         */
        private Integer id;

        /**
         * Schema version
         */
        private Integer version;

        /**
         * Content of the schema
         */
        private String schema;

        /**
         * Schema type
         */
        @Builder.Default
        private SchemaType schemaType = SchemaType.AVRO;

        /**
         * Schema compatibility
         */
        @Builder.Default
        private Compatibility compatibility = Compatibility.GLOBAL;

        /**
         * References list
         */
        private List<Reference> references;

        @Builder
        @AllArgsConstructor
        @NoArgsConstructor
        @Getter
        @Setter
        public static class Reference {
            private String name;
            private String subject;
            private Integer version;
        }
    }

    /**
     * Schema compatibility
     */
    @Introspected
    public enum Compatibility {
        GLOBAL,
        BACKWARD,
        BACKWARD_TRANSITIVE,
        FORWARD,
        FORWARD_TRANSITIVE,
        FULL,
        FULL_TRANSITIVE,
        NONE
    }

    /**
     * Schema type
     */
    @Introspected
    public enum SchemaType {
        AVRO,
        JSON,
        PROTOBUF
    }
}
