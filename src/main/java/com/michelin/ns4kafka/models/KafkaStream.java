package com.michelin.ns4kafka.models;

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
    private static final String apiVersion = "v1";
    public static final String kind = "KafkaStream";

    @Valid
    @NotNull
    private ObjectMeta metadata;
}
