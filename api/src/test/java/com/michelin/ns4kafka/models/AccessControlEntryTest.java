package com.michelin.ns4kafka.models;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AccessControlEntryTest {
    @Test
    void testEquals() {
        AccessControlEntry originalAce = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource("resource1")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .grantedTo("other1")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();
        AccessControlEntry sameAce = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resource("resource1")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .grantedTo("other1")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();
        AccessControlEntry differentAce = AccessControlEntry.builder()
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        // different resource
                        .resource("resource2")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .grantedTo("other1")
                        .permission(AccessControlEntry.Permission.OWNER)
                        .build())
                .build();

        Assertions.assertEquals(originalAce, sameAce);
        Assertions.assertNotEquals(originalAce, differentAce);

    }
}
