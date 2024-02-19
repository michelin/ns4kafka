package com.michelin.ns4kafka.models.schema;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Schema compatibility state.
 */
@Getter
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class SchemaCompatibilityState {
    private static final String apiVersion = "v1";
    public static final String kind = "SchemaCompatibilityState";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private SchemaCompatibilityState.SchemaCompatibilityStateSpec spec;

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
