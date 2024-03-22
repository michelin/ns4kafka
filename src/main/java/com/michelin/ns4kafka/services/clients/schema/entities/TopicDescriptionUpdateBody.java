package com.michelin.ns4kafka.services.clients.schema.entities;

import lombok.Builder;

/**
 * Topic description update body.
 *
 * @param entity entity
 */
@Builder
public record TopicDescriptionUpdateBody(TopicDescriptionUpdateEntity entity) {

}
