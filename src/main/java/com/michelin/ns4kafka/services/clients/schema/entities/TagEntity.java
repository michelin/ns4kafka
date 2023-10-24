package com.michelin.ns4kafka.services.clients.schema.entities;

import java.util.List;
import lombok.Builder;

/**
 * Tag entity.
 *
 * @param displayText The topic name
 * @param classificationNames List of tags
 */
@Builder
public record TagEntity(String displayText, List<String> classificationNames) {
}
