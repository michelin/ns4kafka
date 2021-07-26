package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AccessControlEntry {

    private final String apiVersion = "v1";
    private final String kind = "AccessControlEntry";
    @Valid
    @NotNull
    private ObjectMeta metadata;
    @Valid
    @NotNull
    private AccessControlEntrySpec spec;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class AccessControlEntrySpec {
        @NotNull
        protected ResourceType resourceType;
        @NotNull
        @NotBlank
        protected String resource;
        @NotNull
        protected ResourcePatternType resourcePatternType;
        @NotNull
        protected Permission permission;
        @NotBlank
        @NotNull
        protected String grantedTo;
    }

    /** It's important to follow the same naming as {@link org.apache.kafka.common.resource.ResourceType}
     */
    public enum ResourceType {
        TOPIC,
        GROUP,
        CONNECT,
        SCHEMA,
        STREAM
    }
    /** It's important to follow the same naming as {@link org.apache.kafka.common.resource.ResourcePattern}
     */
    public enum ResourcePatternType {
        LITERAL,
        PREFIXED
    }
    public enum Permission {
        OWNER,
        READ,
        WRITE
    }
}
