package com.michelin.ns4kafka.models.schema;

import static com.michelin.ns4kafka.utils.enums.Kind.SCHEMA_COMPATIBILITY_STATE;

import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Schema compatibility state.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class SchemaCompatibilityState extends MetadataResource {
    @Valid
    @NotNull
    private SchemaCompatibilityState.SchemaCompatibilityStateSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public SchemaCompatibilityState(Metadata metadata,
                                    SchemaCompatibilityState.SchemaCompatibilityStateSpec spec) {
        super("v1", SCHEMA_COMPATIBILITY_STATE, metadata);
        this.spec = spec;
    }

    /**
     * Schema compatibility state spec.
     */
    @Getter
    @Builder
    @ToString
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaCompatibilityStateSpec {
        @Builder.Default
        private final Schema.Compatibility compatibility = Schema.Compatibility.GLOBAL;
    }
}
