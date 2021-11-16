package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class SchemaCompatibilityState {
    private final String apiVersion = "v1";
    private final String kind = "SchemaCompatibilityState";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private SchemaCompatibilityState.SchemaCompatibilityStateSpec spec;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class SchemaCompatibilityStateSpec {
        @Builder.Default
        private Schema.Compatibility compatibility = Schema.Compatibility.DEFAULT;
    }
}
