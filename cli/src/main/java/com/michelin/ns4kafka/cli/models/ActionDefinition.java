package com.michelin.ns4kafka.cli.models;

import io.micronaut.core.annotation.Introspected;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Introspected
@Builder
@Getter
@Setter
public class ActionDefinition {
    private String kind;
    private String path;
    private String names;
    private boolean mustHaveResourceName;
}
