package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

/**
 * Topics list response's entity.
 *
 * @param attributes attributes of the topic
 */
@Builder
public record TopicListResponseEntity(TopicListResponseEntityAttributes attributes) {
}
