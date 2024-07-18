package com.michelin.ns4kafka.model.query;

import java.util.List;
import lombok.Builder;

/**
 * Kafka stream filter parameters in endpoints.
 *
 * @param name kafka stream name filter
 */
@Builder
public record KafkaStreamFilterParams(List<String> name) {
}
