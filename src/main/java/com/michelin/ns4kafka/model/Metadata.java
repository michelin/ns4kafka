package com.michelin.ns4kafka.model;

import static com.michelin.ns4kafka.security.ResourceBasedSecurityRule.RESOURCE_PATTERN;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.Date;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Object metadata.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class Metadata {
    @NotBlank
    @Pattern(regexp = "^" + RESOURCE_PATTERN + "+$")
    private String name;
    private String namespace;
    private String cluster;
    private Map<String, String> labels;
    @EqualsAndHashCode.Exclude
    private int generation;
    @EqualsAndHashCode.Exclude
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Date creationTimestamp;
}
