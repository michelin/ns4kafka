package com.michelin.ns4kafka.models;

import static com.michelin.ns4kafka.models.Kind.KAFKA_STREAM;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Kafka Stream.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class KafkaStream {
    private final String apiVersion = "v1";
    private final String kind = KAFKA_STREAM;

    @Valid
    @NotNull
    private ObjectMeta metadata;
}
