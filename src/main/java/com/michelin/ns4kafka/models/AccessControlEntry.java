package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Access control entry.
 */
@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class AccessControlEntry {
    private static final String apiVersion = "v1";
    public static final String kind = "AccessControlEntry";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private AccessControlEntrySpec spec;

    /**
     * Resource type managed by Ns4kafka.
     */
    public enum ResourceType {
        TOPIC,
        GROUP,
        CONNECT,
        CONNECT_CLUSTER,
        SCHEMA,
        TRANSACTIONAL_ID
    }

    /**
     * Resource pattern type.
     */
    public enum ResourcePatternType {
        LITERAL,
        PREFIXED
    }

    /**
     * Permission.
     */
    public enum Permission {
        OWNER,
        READ,
        WRITE
    }

    /**
     * Access control entry specification.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccessControlEntrySpec {
        @NotNull
        protected AccessControlEntry.ResourceType resourceType;

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
}
