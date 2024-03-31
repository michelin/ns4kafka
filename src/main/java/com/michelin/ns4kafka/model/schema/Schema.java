package com.michelin.ns4kafka.model.schema;

import static com.michelin.ns4kafka.util.enumation.Kind.SCHEMA;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Schema.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class Schema extends MetadataResource {
    @Valid
    @NotNull
    private SchemaSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public Schema(Metadata metadata, SchemaSpec spec) {
        super("v1", SCHEMA, metadata);
        this.spec = spec;
    }

    /**
     * Schema compatibility.
     */
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
     * Schema type.
     */
    public enum SchemaType {
        AVRO,
        JSON,
        PROTOBUF
    }

    /**
     * Schema spec.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaSpec {
        private Integer id;
        private Integer version;
        private String schema;

        @Builder.Default
        private SchemaType schemaType = SchemaType.AVRO;

        @Builder.Default
        private Compatibility compatibility = Compatibility.GLOBAL;
        private List<Reference> references;

        /**
         * Schema reference.
         */
        @Getter
        @Setter
        @Builder
        @Introspected
        @NoArgsConstructor
        @AllArgsConstructor
        public static class Reference {
            private String name;
            private String subject;
            private Integer version;
        }
    }
}
