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
    /**
     * The API version
     */
    private final String apiVersion = "v1";

    /**
     * The resource kind
     */
    private final String kind = "AccessControlEntry";

    /**
     * The resource metadata
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * The resource specification
     */
    @Valid
    @NotNull
    private AccessControlEntrySpec spec;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class AccessControlEntrySpec {
        /**
         * The ACL type
         */
        @NotNull
        protected ResourceType resourceType;

        /**
         * The resource name
         */
        @NotNull
        @NotBlank
        protected String resource;

        /**
         * The pattern type
         */
        @NotNull
        protected ResourcePatternType resourcePatternType;

        /**
         * The permission type
         */
        @NotNull
        protected Permission permission;

        /**
         * The grantee
         */
        @NotBlank
        @NotNull
        protected String grantedTo;
    }

    /**
     * The resource types
     * It's important to follow the same naming as {@link org.apache.kafka.common.resource.ResourceType}
     */
    public enum ResourceType {
        TOPIC,
        GROUP,
        CONNECT,
        SCHEMA
    }

    /**
     * The resource patterns
     * It's important to follow the same naming as {@link org.apache.kafka.common.resource.ResourcePattern}
     */
    public enum ResourcePatternType {
        LITERAL,
        PREFIXED
    }

    /**
     * The permissions
     */
    public enum Permission {
        OWNER,
        READ,
        WRITE
    }
}
