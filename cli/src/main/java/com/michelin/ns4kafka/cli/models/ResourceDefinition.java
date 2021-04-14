package com.michelin.ns4kafka.cli.models;

import java.util.List;

import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;

@Introspected
@Getter
@Setter
public class ResourceDefinition {
    private String kind;
    private boolean namespaced;
    private String path;
    private List<String> names;
}
