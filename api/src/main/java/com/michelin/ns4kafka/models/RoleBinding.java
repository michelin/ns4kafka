package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.List;

@Introspected
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class RoleBinding {

    private final String apiVersion = "v1";
    private final String kind = "RoleBinding";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private RoleBindingSpec spec;


    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Schema(description = "Contains the Role Binding specification")
    public static class RoleBindingSpec {

        @NotNull
        @NotBlank
        private Role role;

        @NotNull
        @NotBlank
        private Subject subject;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class Role {

        @NotNull
        private Collection<ResourceType> resourceTypes;

        @NotNull
        private Collection<Verb> verbs;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class Subject {

        @NotNull
        private SubjectType subjectType;

        @NotNull
        private String subjectName;

    }

    public enum ResourceType {
        topics,
        connects,
        schemas,
        consumer_groups,
        acls
    }

    public enum Verb {
        GET,
        POST,
        PUT,
        DELETE
    }

    public enum SubjectType {
        GROUP,
        USER
    }
}
