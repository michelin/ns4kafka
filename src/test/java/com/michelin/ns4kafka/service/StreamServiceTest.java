package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.repository.StreamRepository;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamServiceTest {
    @InjectMocks
    StreamService streamService;

    @Mock
    AclService aclService;

    @Mock
    StreamRepository streamRepository;

    @Test
    void findAllEmpty() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(streamRepository.findAllForCluster("local"))
            .thenReturn(List.of());
        var actual = streamService.findAllForNamespace(ns);
        assertTrue(actual.isEmpty());
    }

    @Test
    void findAll() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .namespace("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream2 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream2")
                .namespace("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream3 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream3")
                .namespace("test")
                .cluster("local")
                .build())
            .build();


        when(streamRepository.findAllForCluster("local"))
            .thenReturn(List.of(stream1, stream2, stream3));
        var actual = streamService.findAllForNamespace(ns);
        assertEquals(3, actual.size());
        assertTrue(actual.contains(stream1));
        assertTrue(actual.contains(stream2));
        assertTrue(actual.contains(stream3));
    }

    @Test
    void findByName() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream1 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream1")
                .namespace("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream2 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream2")
                .namespace("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream3 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test_stream3")
                .namespace("test")
                .cluster("local")
                .build())
            .build();


        when(streamRepository.findAllForCluster("local"))
            .thenReturn(List.of(stream1, stream2, stream3));
        var actual = streamService.findByName(ns, "test_stream2");
        assertTrue(actual.isPresent());
        assertEquals(stream2, actual.get());
    }

    @Test
    void findByNameEmpty() {

        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        when(streamRepository.findAllForCluster("local"))
            .thenReturn(List.of());
        var actual = streamService.findByName(ns, "test_stream2");
        assertTrue(actual.isEmpty());
    }

    @Test
    void isNamespaceOwnerOfKafkaStream() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("test")
                .cluster("local")
                .build())
            .build();

        AccessControlEntry ace1 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("test.")
                .grantedTo("test")
                .build()
            )
            .build();
        AccessControlEntry ace2 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.GROUP)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("test.")
                .grantedTo("test")
                .build()
            )
            .build();
        AccessControlEntry ace3 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.CONNECT)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("test.")
                .grantedTo("test")
                .build()
            )
            .build();
        AccessControlEntry ace4 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("test-bis.")
                .grantedTo("test")
                .build()
            )
            .build();
        AccessControlEntry ace5 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.GROUP)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.LITERAL)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("test-bis.")
                .grantedTo("test")
                .build()
            )
            .build();
        AccessControlEntry ace6 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.GROUP)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("test-ter.")
                .grantedTo("test")
                .build()
            )
            .build();
        AccessControlEntry ace7 = AccessControlEntry.builder()
            .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                .resourceType(AccessControlEntry.ResourceType.TOPIC)
                .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                .permission(AccessControlEntry.Permission.OWNER)
                .resource("test-qua.")
                .grantedTo("test")
                .build()
            )
            .build();

        when(aclService.findAllGrantedToNamespace(ns))
            .thenReturn(List.of(ace1, ace2, ace3, ace4, ace5, ace6, ace7));

        assertTrue(
            streamService.isNamespaceOwnerOfKafkaStream(ns, "test.stream"));
        Assertions.assertFalse(
            streamService.isNamespaceOwnerOfKafkaStream(ns, "test-bis.stream"), "ACL are LITERAL");
        Assertions.assertFalse(
            streamService.isNamespaceOwnerOfKafkaStream(ns, "test-ter.stream"), "Topic ACL missing");
        Assertions.assertFalse(
            streamService.isNamespaceOwnerOfKafkaStream(ns, "test-qua.stream"), "Group ACL missing");
        Assertions.assertFalse(
            streamService.isNamespaceOwnerOfKafkaStream(ns, "test-nop.stream"), "No ACL");
    }
}
