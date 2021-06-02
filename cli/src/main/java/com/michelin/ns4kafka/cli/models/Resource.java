package com.michelin.ns4kafka.cli.models;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@Introspected
@Getter
@Setter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Resource {

    private String apiVersion;
    private String kind;
    private ObjectMeta metadata;
    private Map<String,Object> spec;
    private Object status;
}
