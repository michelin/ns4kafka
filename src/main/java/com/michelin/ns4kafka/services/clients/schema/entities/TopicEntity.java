package com.michelin.ns4kafka.services.clients.schema.entities;

import java.util.List;
import lombok.Builder;

/**
 * Topics list response's entity.
 *
 * @param attributes attributes of the topic
 */
@Builder
public record TopicEntity(TopicEntityAttributes attributes, List<String> classificationNames) {

}
