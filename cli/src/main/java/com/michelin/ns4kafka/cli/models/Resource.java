package com.michelin.ns4kafka.cli.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Introspected
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Resource {
    private String apiVersion;
    private String kind;
    private ObjectMeta metadata;
    @JsonInclude(value = JsonInclude.Include.NON_ABSENT)
    private Map<String,Object> spec;
    private Object status;
}
