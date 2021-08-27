package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Introspected
@Data
@EqualsAndHashCode(callSuper = true)
public class KafkaStream extends Resource{

    @Builder
    public KafkaStream(@NotNull ObjectMeta metadata) {
        super("v1","KafkaStream", metadata);
    }
}
