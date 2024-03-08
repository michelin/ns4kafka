package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

/**
 * Attribute entities.
 *
 * @param attributes attributes of the topic
 * @param typeName topic type name
 */
@Builder
public record TopicDescriptionUpdateEntity(TopicDescriptionUpdateAttributes attributes, String typeName) {

}
