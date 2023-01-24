package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
                        // different resource
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
                        .grantedTo("other1")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();
        AccessControlEntry differentByPermission = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource("resource1")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .grantedTo("other2")
                        .permission(AccessControlEntry.Permission.READ)
                        .build())
                .build();
        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(original, differentByResource);

    }
}
