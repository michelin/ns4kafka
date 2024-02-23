package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.utils.enums.Kind;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Metadata resource.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class MetadataResource extends Resource {
    @Valid
    @NotNull
    private Metadata metadata;

    /**
     * Constructor.
     *
     * @param version  The version
     * @param metadata The metadata
     */
    public MetadataResource(String version, Kind kind, Metadata metadata) {
        super(version, kind);
        this.metadata = metadata;
    }
}
