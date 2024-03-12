package com.michelin.ns4kafka.models;

import static com.michelin.ns4kafka.utils.enums.Kind.ACCESS_CONTROL_ENTRY;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Access control entry.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class AccessControlEntry extends MetadataResource {
    @Valid
    @NotNull
    private AccessControlEntrySpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public AccessControlEntry(Metadata metadata, AccessControlEntrySpec spec) {
        super("v1", ACCESS_CONTROL_ENTRY, metadata);
        this.spec = spec;
    }

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
