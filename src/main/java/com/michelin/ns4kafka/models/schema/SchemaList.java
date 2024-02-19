package com.michelin.ns4kafka.models.schema;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Schema list.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class SchemaList {
    private static final String apiVersion = "v1";
    public static final String kind = "SchemaList";

    @Valid
    @NotNull
    private ObjectMeta metadata;
}
