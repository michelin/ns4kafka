package com.michelin.ns4kafka.services.clients.schema.entities;

import java.util.List;
import lombok.Builder;

/**
 * Tag entities.
 *
 * @param entities List of Tag entity
 */
@Builder
public record TagEntities(List<TagEntity> entities) {
}
