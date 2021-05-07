package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.constraints.NotBlank;
import java.util.Date;
import java.util.Map;

@Introspected
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class ObjectMeta {
    @NotBlank
    private String name;
    private String namespace;
    private String cluster;
    private Map<String,String> labels;
    private int generation;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Date creationTimestamp;

    @Override
    public String toString() {
        return "ObjectMeta{" +
                "name='" + name + '\'' +
                ", namespace='" + namespace + '\'' +
                ", cluster='" + cluster + '\'' +
                '}';
    }
}
