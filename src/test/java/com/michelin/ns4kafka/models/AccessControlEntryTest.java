package com.michelin.ns4kafka.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

/**
 * Access control entry test.
 */
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
