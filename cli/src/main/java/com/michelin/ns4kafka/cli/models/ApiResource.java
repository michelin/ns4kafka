package com.michelin.ns4kafka.cli.models;

import io.micronaut.core.annotation.Introspected;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Introspected
@Getter
@Setter
public class ApiResource {
    private String kind;
    private boolean namespaced;
    private boolean synchronizable;
    private String path;
    private List<String> names;
}
