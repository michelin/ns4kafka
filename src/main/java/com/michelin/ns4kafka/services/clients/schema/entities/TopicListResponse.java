package com.michelin.ns4kafka.services.clients.schema.entities;

import java.util.List;
import lombok.Builder;

/**
 * Topics list response's entities.
 *
 * @param entities List of entities
 *
 */
@Builder
public record TopicListResponse(List<TopicListResponseEntity> entities) {
}
