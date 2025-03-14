/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class RoleBindingTest {
    @Test
    void shouldRoleBeEqual() {
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

        assertEquals(original, same);

        assertNotEquals(original, differentByResourceTypes);
        assertNotEquals(original, differentByVerbs);
    }

    @Test
    void shouldSubjectBeEqual() {
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

        assertEquals(original, same);

        assertNotEquals(original, differentByName);
        assertNotEquals(original, differentByType);
    }

    @Test
    void shouldRoleBindingBeEqual() {
        RoleBinding original = RoleBinding.builder()
                .metadata(Metadata.builder().name("rb1").build())
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
                .metadata(Metadata.builder().name("rb1").build())
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

        assertEquals(original, same);

        RoleBinding differentByMetadata = RoleBinding.builder()
                .metadata(Metadata.builder().name("rb1").cluster("cluster").build())
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

        assertNotEquals(original, differentByMetadata);

        RoleBinding differentByRole = RoleBinding.builder()
                .metadata(Metadata.builder().name("rb1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder().build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("subject1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        assertNotEquals(original, differentByRole);

        RoleBinding differentBySubject = RoleBinding.builder()
                .metadata(Metadata.builder().name("rb1").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("res1", "res2"))
                                .verbs(List.of(RoleBinding.Verb.GET, RoleBinding.Verb.POST))
                                .build())
                        .subject(RoleBinding.Subject.builder().build())
                        .build())
                .build();

        assertNotEquals(original, differentBySubject);
    }
}
