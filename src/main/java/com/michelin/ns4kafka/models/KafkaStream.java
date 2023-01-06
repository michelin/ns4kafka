package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class KafkaStream {
    private final String apiVersion = "v1";
    private final String kind = "KafkaStream";

    @Valid
    @NotNull
    private ObjectMeta metadata;
}
