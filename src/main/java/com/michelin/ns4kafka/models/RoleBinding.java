package com.michelin.ns4kafka.models;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collection;
import java.util.List;
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
    @NoArgsConstructor
    @Getter
    @Setter
    public static class Role {
        private Collection<String> resourceTypes = List.of("topics","connects","schemas","consumer-groups");
        private Collection<String> verbs = List.of("GET","POST","PUT","DELETE");
    }

    @NoArgsConstructor
    @Getter
    @Setter
    public static class Subject {
        private SubjectType subjectType = SubjectType.GROUP;
        private String subjectName;
        public Subject(String group){
            this.subjectName = group;
        }
    }

    public enum SubjectType {
        GROUP,
        USER
    }
}
