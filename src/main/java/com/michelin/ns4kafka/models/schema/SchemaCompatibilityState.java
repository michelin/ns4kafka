package com.michelin.ns4kafka.models.schema;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Builder
@Getter
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class SchemaCompatibilityState {
    private final String apiVersion = "v1";
    private final String kind = "SchemaCompatibilityState";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private SchemaCompatibilityState.SchemaCompatibilityStateSpec spec;

    @Getter
    @Builder
    @ToString
    @Introspected
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SchemaCompatibilityStateSpec {
        @Builder.Default
        private final Schema.Compatibility compatibility = Schema.Compatibility.GLOBAL;
    }
}
