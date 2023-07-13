package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class AccessControlEntryTest {
    @Test
    void testEquals() {
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

        AccessControlEntry differentByResource = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource("resource2")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .grantedTo("other1")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();

        AccessControlEntry differentByResourcePatternType = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource("resource1")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .grantedTo("other1")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();

        AccessControlEntry differentByResourceType = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource("resource1")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.CONNECT)
                        .grantedTo("other1")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();

        AccessControlEntry differentByGrantedTo = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource("resource1")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .grantedTo("other2")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();

        AccessControlEntry differentByPermission = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource("resource1")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .grantedTo("other1")
                        .permission(AccessControlEntry.Permission.READ)
                        .build())
                .build();

        assertEquals(original, same);
        assertNotEquals(original, differentByResource);
        assertNotEquals(original, differentByResourcePatternType);
        assertNotEquals(original, differentByResourceType);
        assertNotEquals(original, differentByGrantedTo);
        assertNotEquals(original, differentByPermission);
    }
}
