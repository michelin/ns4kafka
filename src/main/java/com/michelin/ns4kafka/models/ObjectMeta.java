package com.michelin.ns4kafka.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.Date;
import java.util.Map;

@Data
@Builder
@Introspected
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
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Date creationTimestamp;

}
