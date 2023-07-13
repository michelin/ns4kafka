package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.acl.AclNonNamespacedController;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AclNonNamespacedControllerTest {
    /**
     * The mocked ACL service
     */
    @Mock
    AccessControlEntryService accessControlEntryService;

    /**
     * The mocked ACL controller
     */
    @InjectMocks
    AclNonNamespacedController aclNonNamespacedController;

    @Test
    void listAll() {
        AccessControlEntry ace1 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("namespace1").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder().grantedTo("namespace1").build()).build();
        AccessControlEntry ace2 = AccessControlEntry.builder()
                .metadata(ObjectMeta.builder().namespace("namespace2").build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder().grantedTo("namespace2").build()).build();

        when(accessControlEntryService.findAll()).thenReturn(List.of(ace1, ace2));

        List<AccessControlEntry> actual = aclNonNamespacedController.listAll();

        assertEquals(2, actual.size());
        assertEquals(List.of(ace1, ace2), actual);
    }
}
