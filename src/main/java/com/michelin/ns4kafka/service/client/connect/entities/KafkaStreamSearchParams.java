package com.michelin.ns4kafka.service.client.connect.entities;

import java.util.List;
import lombok.Builder;

/**
 * Kafka stream search parameters in endpoints.
 *
 * @param name kafka stream name filter
 */
@Builder
public record KafkaStreamSearchParams(List<String> name) {
}
