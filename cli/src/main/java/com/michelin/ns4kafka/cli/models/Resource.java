package com.michelin.ns4kafka.cli.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import java.util.Map;

@Data
@Getter
@Builder
@Introspected
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
