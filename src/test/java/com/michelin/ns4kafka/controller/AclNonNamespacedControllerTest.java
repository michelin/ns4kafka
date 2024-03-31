package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controller.acl.AclNonNamespacedController;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.service.AccessControlEntryService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AclNonNamespacedControllerTest {
    @Mock
    AccessControlEntryService accessControlEntryService;

    @InjectMocks
    AclNonNamespacedController aclNonNamespacedController;

    @Test
    void listAll() {
        AccessControlEntry ace1 = AccessControlEntry.builder()
            .metadata(Metadata.builder().namespace("namespace1").build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder().grantedTo("namespace1").build()).build();
        AccessControlEntry ace2 = AccessControlEntry.builder()
            .metadata(Metadata.builder().namespace("namespace2").build())
            .spec(AccessControlEntry.AccessControlEntrySpec.builder().grantedTo("namespace2").build()).build();

        when(accessControlEntryService.findAll()).thenReturn(List.of(ace1, ace2));

        List<AccessControlEntry> actual = aclNonNamespacedController.listAll();

        assertEquals(2, actual.size());
        assertEquals(List.of(ace1, ace2), actual);
    }
}
