package com.michelin.ns4kafka.models;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class KafkaStream {
    private final String apiVersion = "v1";
    private final String kind = "KafkaStream";

    @Valid
    @NotNull
    private ObjectMeta metadata;
}
