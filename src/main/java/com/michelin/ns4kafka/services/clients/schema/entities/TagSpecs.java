package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

/**
 * Tag Specs to call schema registry API.
 *
 * @param entityName The entity name
 * @param entityType The entity type
 * @param typeName The type name
 */
@Builder
public record TagSpecs(String entityName, String entityType, String typeName) {

    @Override
    public String toString() {
        return entityName + "/" + typeName;
    }

}
