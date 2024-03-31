package com.michelin.ns4kafka.model.schema;

import static com.michelin.ns4kafka.util.enumation.Kind.SCHEMA_LIST;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Schema list.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class SchemaList extends MetadataResource {
    /**
     * Constructor.
     *
     * @param metadata The metadata
     */
    @Builder
    public SchemaList(Metadata metadata) {
        super("v1", SCHEMA_LIST, metadata);
    }
}
