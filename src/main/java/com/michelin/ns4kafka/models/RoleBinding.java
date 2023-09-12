package com.michelin.ns4kafka.models;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;


@Data
@Builder
@Serdeable
@AllArgsConstructor
@NoArgsConstructor
public class RoleBinding {
    private final String apiVersion = "v1";
    private final String kind = "RoleBinding";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private RoleBindingSpec spec;

    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RoleBindingSpec {
        @Valid
        @NotNull
        private Role role;

        @Valid
        @NotNull
        private Subject subject;
    }

    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Role {
        @NotNull
        @NotEmpty
        private Collection<String> resourceTypes;

        @NotNull
        @NotEmpty
        private Collection<Verb> verbs;
    }

    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
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
