package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Collection;


@Introspected(classes = {RoleBinding.class, RoleBinding.RoleBindingSpec.class, RoleBinding.Role.class, RoleBinding.Subject.class})
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class RoleBinding extends Resource{

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
    @Data
    public static class RoleBindingSpec {

        @Valid
        @NotNull
        private Role role;

        @Valid
        @NotNull
        private Subject subject;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Role {

        @NotNull
        @NotEmpty
        private Collection<String> resourceTypes;

        @NotNull
        @NotEmpty
        private Collection<Verb> verbs;
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Subject {

        @NotNull
        private SubjectType subjectType;

        @NotNull
        @NotBlank
        private String subjectName;

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
