package com.michelin.ns4kafka.models;

import lombok.*;

import java.util.Collection;
import java.util.List;
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class RoleBinding {
    private String namespace;
    private Role role; //TODO Collection<Role>
    private Subject subject; //TODO Collection<Subject>

    public RoleBinding(String namespace, String group){
        this.namespace=namespace;
        this.role = new Role();
        this.subject = new Subject(group);
    }

    //TODO RoleRef instead: roleAdmin, roleRead, roleXXX + RoleRepository
    @Builder
    @AllArgsConstructor
    @Getter
    @Setter
    public static class Role {
        private final Collection<String> resourceTypes = List.of("topics","connects","schemas","consumer-groups");
        private final Collection<String> verbs = List.of("GET","POST","PUT","DELETE");
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class Subject {
        private final SubjectType subjectType = SubjectType.GROUP;
        private String subjectName;

    }

    public enum SubjectType {
        GROUP,
        USER
    }
}
