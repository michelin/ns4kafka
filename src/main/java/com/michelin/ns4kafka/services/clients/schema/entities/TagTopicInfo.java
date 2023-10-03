package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

/**
 * Information on tag.
 *
 * @param entityName The entity name
 * @param entityType The entity type
 * @param typeName The type name
 * @param entityStatus The entity status
 */
@Builder
public record TagTopicInfo(String entityName, String entityType, String typeName, String entityStatus) {

    @Override
    public String toString() {
        return entityName + "/" + typeName;
    }

}
