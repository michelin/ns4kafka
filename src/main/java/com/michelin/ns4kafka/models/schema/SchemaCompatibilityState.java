package com.michelin.ns4kafka.models.schema;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Builder
@Getter
@Serdeable
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
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaCompatibilityStateSpec {
        @Builder.Default
        private final Schema.Compatibility compatibility = Schema.Compatibility.GLOBAL;
    }
}
