package com.michelin.ns4kafka.cli.models;

import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;

@Introspected
@Getter
@Setter
public class Resource {

    private String apiVersion;
    private String kind;
    private ObjectMeta metadata;
    private Object spec;
}
