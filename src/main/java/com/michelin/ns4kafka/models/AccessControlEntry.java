package com.michelin.ns4kafka.models;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class AccessControlEntry {
    private final String apiVersion = "v1";
    private final String kind = "AccessControlEntry";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private AccessControlEntrySpec spec;

    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
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

    public enum ResourceType {
        TOPIC,
        GROUP,
        CONNECT,
        CONNECT_CLUSTER,
        SCHEMA,
        TRANSACTIONAL_ID
    }

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
