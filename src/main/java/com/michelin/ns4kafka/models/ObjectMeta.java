package com.michelin.ns4kafka.models;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.*;

import java.util.Date;
import java.util.Map;

@Data
@Builder
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class ObjectMeta {
    @NotBlank
    @Pattern(regexp = "^[a-zA-Z0-9_.-]+$")
    private String name;
    private String namespace;
    private String cluster;
    private Map<String,String> labels;
    @EqualsAndHashCode.Exclude
    private int generation;
    @EqualsAndHashCode.Exclude
    private Date creationTimestamp;

}
