package com.michelin.ns4kafka.models;


import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Introspected
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public abstract class KafkaResource {
    private String apiVersion;
    private String kind;
    @Valid
    @NotNull
    private ObjectMeta metadata;
}
