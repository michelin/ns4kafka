package com.michelin.ns4kafka.models;

import static com.michelin.ns4kafka.utils.enums.Kind.ROLE_BINDING;

import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Collection;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Role binding.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class RoleBinding extends MetadataResource {
    @Valid
    @NotNull
    private RoleBindingSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public RoleBinding(Metadata metadata, RoleBindingSpec spec) {
        super("v1", ROLE_BINDING, metadata);
        this.spec = spec;
    }

    /**
     * HTTP verbs.
     */
    public enum Verb {
        GET,
        POST,
        PUT,
        DELETE
    }

    /**
     * Subject type.
     */
    public enum SubjectType {
        GROUP,
        USER
    }

    /**
     * Role binding spec.
     */
    @Data
    @Builder
    @Introspected
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

    /**
     * Role.
     */
    @Data
    @Builder
    @Introspected
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

    /**
     * Subject.
     */
    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Subject {
        @NotNull
        private SubjectType subjectType;

        @NotNull
        @NotBlank
        private String subjectName;
    }
}
