package com.michelin.ns4kafka.services.clients.schema.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

/**
 * Attribute entities.
 *
 * @param entity entity
 */
@Builder
public record TopicDescriptionUpdateBody(TopicDescriptionUpdateEntity entity) {

}
