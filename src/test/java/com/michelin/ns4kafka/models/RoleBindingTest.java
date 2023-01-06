package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RoleBindingTest {
    @Test
    void testEquals_Role() {
        RoleBinding.Role original = RoleBinding.Role.builder()
                .resourceTypes(List.of("res1", "res2"))
                .verbs(List.of(RoleBinding.Verb.GET, RoleBinding.Verb.POST))
                .build();
        RoleBinding.Role same = RoleBinding.Role.builder()
                .resourceTypes(List.of("res1", "res2"))
                .verbs(List.of(RoleBinding.Verb.GET, RoleBinding.Verb.POST))
                .build();
        RoleBinding.Role differentByResourceTypes = RoleBinding.Role.builder()
                .resourceTypes(List.of("res1", "res2", "res3"))
                .verbs(List.of(RoleBinding.Verb.GET, RoleBinding.Verb.POST))
                .build();
        RoleBinding.Role differentByVerbs = RoleBinding.Role.builder()
                .resourceTypes(List.of("res1", "res2", "res3"))
                .verbs(List.of(RoleBinding.Verb.DELETE))
                .build();

        Assertions.assertEquals(original, same);

        Assertions.assertNotEquals(original, differentByResourceTypes);
        Assertions.assertNotEquals(original, differentByVerbs);
    }

    @Test
    void testEquals_Subject() {
        RoleBinding.Subject original = RoleBinding.Subject.builder()
                .subjectName("subject1")
                .subjectType(RoleBinding.SubjectType.GROUP)
                .build();
        RoleBinding.Subject same = RoleBinding.Subject.builder()
                .subjectName("subject1")
                .subjectType(RoleBinding.SubjectType.GROUP)
                .build();
        RoleBinding.Subject differentByName = RoleBinding.Subject.builder()
                .subjectName("subject2")
                .subjectType(RoleBinding.SubjectType.GROUP)
                .build();
        RoleBinding.Subject differentByType = RoleBinding.Subject.builder()
                .subjectName("subject1")
                .subjectType(RoleBinding.SubjectType.USER)
                .build();

        Assertions.assertEquals(original, same);

        Assertions.assertNotEquals(original, differentByName);
        Assertions.assertNotEquals(original, differentByType);
    }

    @Test
    void testEquals_RoleBinding() {
        RoleBinding original = RoleBinding.builder()
                .metadata(ObjectMeta.builder().name("rb1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("res1", "res2"))
                                .verbs(List.of(RoleBinding.Verb.GET, RoleBinding.Verb.POST))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("subject1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();
        RoleBinding same = RoleBinding.builder()
                .metadata(ObjectMeta.builder().name("rb1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("res1", "res2"))
                                .verbs(List.of(RoleBinding.Verb.GET, RoleBinding.Verb.POST))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("subject1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();
        RoleBinding differentByMetadata = RoleBinding.builder()
                .metadata(ObjectMeta.builder().name("rb1").cluster("cluster").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("res1", "res2"))
                                .verbs(List.of(RoleBinding.Verb.GET, RoleBinding.Verb.POST))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("subject1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();
        RoleBinding differentByRole = RoleBinding.builder()
                .metadata(ObjectMeta.builder().name("rb1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder().build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("subject1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();
        RoleBinding differentBySubject = RoleBinding.builder()
                .metadata(ObjectMeta.builder().name("rb1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("res1", "res2"))
                                .verbs(List.of(RoleBinding.Verb.GET, RoleBinding.Verb.POST))
                                .build())
                        .subject(RoleBinding.Subject.builder().build())
                        .build())
                .build();

        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(original, differentByMetadata);
        Assertions.assertNotEquals(original, differentByRole);
        Assertions.assertNotEquals(original, differentBySubject);
    }
}
