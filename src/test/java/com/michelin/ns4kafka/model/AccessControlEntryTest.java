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

import org.junit.jupiter.api.Test;

class AccessControlEntryTest {
    @Test
    void shouldBeEqual() {
        AccessControlEntry original = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resource("resource1")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .grantedTo("other1")
                .permission(AccessControlEntry.Permission.OWNER)
                .build())
            .build();

        AccessControlEntry same = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resource("resource1")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .grantedTo("other1")
                .permission(AccessControlEntry.Permission.OWNER)
                .build())
            .build();

        assertEquals(original, same);

        AccessControlEntry differentByResource = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resource("resource2")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .grantedTo("other1")
                .permission(AccessControlEntry.Permission.OWNER)
                .build())
            .build();

        assertNotEquals(original, differentByResource);

        AccessControlEntry differentByResourcePatternType = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resource("resource1")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .grantedTo("other1")
                .permission(AccessControlEntry.Permission.OWNER)
                .build())
            .build();

        assertNotEquals(original, differentByResourcePatternType);

        AccessControlEntry differentByResourceType = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resource("resource1")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                .grantedTo("other1")
                .permission(AccessControlEntry.Permission.OWNER)
                .build())
            .build();

        assertNotEquals(original, differentByResourceType);

        AccessControlEntry differentByGrantedTo = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resource("resource1")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .grantedTo("other2")
                .permission(AccessControlEntry.Permission.OWNER)
                .build())
            .build();

        assertNotEquals(original, differentByGrantedTo);

        AccessControlEntry differentByPermission = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resource("resource1")
                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .grantedTo("other1")
                .permission(AccessControlEntry.Permission.READ)
                .build())
            .build();

        assertNotEquals(original, differentByPermission);
    }
}
