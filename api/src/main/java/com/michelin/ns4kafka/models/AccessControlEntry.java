package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Introspected
@Data
@EqualsAndHashCode(callSuper = true)
public class AccessControlEntry extends Resource{

    @Builder
    public AccessControlEntry(@NotNull ObjectMeta metadata, AccessControlEntrySpec spec) {
        super("v1","AccessControlEntry", metadata);
        this.spec = spec;
    }

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
        SCHEMA
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
