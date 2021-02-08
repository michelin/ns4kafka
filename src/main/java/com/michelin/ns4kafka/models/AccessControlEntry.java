package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class AccessControlEntry {
    public static final String ADMIN = "admin";

    private final String apiVersion = "v1";
    private final String kind = "AccessControlEntry";
    // No validation here since name and labels are set server-side
    private ObjectMeta metadata;

    @Valid
    @NotNull
    AccessControlEntrySpec spec;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Schema(description = "Contains the Topic specification")
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
    public static List<AccessControlEntry> buildGeneric(String namespace, String cluster, String prefix){
        ObjectMeta metadata = ObjectMeta.builder()
                .namespace(namespace)
                .cluster(cluster)
                .labels(Map.of("grantedBy", AccessControlEntry.ADMIN))
                .creationTimestamp(Date.from(Instant.now()))
                .build();

        return List.of(
                AccessControlEntry
                        .builder()
                        .spec(AccessControlEntrySpec.builder()
                                .resourceType(ResourceType.TOPIC)
                                .resource(prefix)
                                .resourcePatternType(ResourcePatternType.PREFIXED)
                                .permission(Permission.OWNER)
                                .grantedTo(namespace)
                                .build())
                        .metadata(metadata)
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntrySpec.builder()
                                .resourceType(ResourceType.GROUP)
                                .resource(prefix)
                                .resourcePatternType(ResourcePatternType.PREFIXED)
                                .permission(Permission.OWNER)
                                .grantedTo(namespace)
                                .build())
                        .metadata(metadata)
                        .build(),
                AccessControlEntry.builder()
                        .spec(AccessControlEntrySpec.builder()
                                .resourceType(ResourceType.GROUP)
                                .resource("connect-"+prefix)
                                .resourcePatternType(ResourcePatternType.PREFIXED)
                                .permission(Permission.OWNER)
                                .grantedTo(namespace)
                                .build())
                        .metadata(metadata)
                        .build()
        );
    }
}
