package com.michelin.ns4kafka.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.KafkaStream;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.repository.StreamRepository;
import com.michelin.ns4kafka.service.client.connect.entities.KafkaStreamSearchParams;
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
    AccessControlEntryService accessControlEntryService;

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
    void findKafkaStreamWithNameParameter() {
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

        when(streamRepository.findAllForCluster("local")).thenReturn(List.of(stream1, stream2, stream3));

        KafkaStreamSearchParams params1 = KafkaStreamSearchParams.builder().name(List.of("test_stream3")).build();
        List<KafkaStream> list1 = streamService.findAllForNamespace(ns, params1);
        assertEquals(List.of(stream3), list1);

        KafkaStreamSearchParams params2 = KafkaStreamSearchParams.builder().name(List.of("test_stream5")).build();
        List<KafkaStream> list2 = streamService.findAllForNamespace(ns, params2);
        assertTrue(list2.isEmpty());

        KafkaStreamSearchParams params3 = KafkaStreamSearchParams.builder().name(List.of("")).build();
        List<KafkaStream> list3 = streamService.findAllForNamespace(ns, params3);
        assertEquals(List.of(stream1, stream2, stream3), list3);
    }

    @Test
    void findKafkaStreamWithWildcardNameParameter() {
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
        KafkaStream stream4 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test.stream1")
                .namespace("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream5 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("stream2_test")
                .namespace("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream6 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("prefix.stream_test1")
                .namespace("test2")
                .cluster("local")
                .build())
            .build();

        when(streamRepository.findAllForCluster("local"))
            .thenReturn(List.of(stream1, stream2, stream3, stream4, stream5));

        KafkaStreamSearchParams params1 = KafkaStreamSearchParams.builder().name(List.of("test_*")).build();
        assertEquals(List.of(stream1, stream2, stream3), streamService.findAllForNamespace(ns, params1));

        KafkaStreamSearchParams params2 = KafkaStreamSearchParams.builder().name(List.of("test_stream?")).build();
        assertEquals(List.of(stream1, stream2, stream3), streamService.findAllForNamespace(ns, params2));

        KafkaStreamSearchParams params3 = KafkaStreamSearchParams.builder().name(List.of("*_*")).build();
        assertEquals(List.of(stream1, stream2, stream3, stream5), streamService.findAllForNamespace(ns, params3));

        KafkaStreamSearchParams params4 = KafkaStreamSearchParams.builder().name(List.of("test?stream1")).build();
        assertEquals(List.of(stream1, stream4), streamService.findAllForNamespace(ns, params4));

        KafkaStreamSearchParams params5 = KafkaStreamSearchParams.builder().name(List.of("*stream2*")).build();
        assertEquals(List.of(stream2, stream5), streamService.findAllForNamespace(ns, params5));

        KafkaStreamSearchParams params6 = KafkaStreamSearchParams.builder().name(List.of("*stream5")).build();
        assertTrue(streamService.findAllForNamespace(ns, params6).isEmpty());

        KafkaStreamSearchParams params7 = KafkaStreamSearchParams.builder().name(List.of("test??stream1")).build();
        assertTrue(streamService.findAllForNamespace(ns, params7).isEmpty());

        KafkaStreamSearchParams params8 = KafkaStreamSearchParams.builder().name(List.of(".*")).build();
        assertTrue(streamService.findAllForNamespace(ns, params8).isEmpty());

        KafkaStreamSearchParams params9 = KafkaStreamSearchParams.builder().name(List.of("*")).build();
        assertEquals(List.of(stream1, stream2, stream3, stream4, stream5),
            streamService.findAllForNamespace(ns, params9));
    }

    @Test
    void findKafkaStreamWithMultipleNameParameters() {
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
        KafkaStream stream4 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("test.stream1")
                .namespace("test")
                .cluster("local")
                .build())
            .build();
        KafkaStream stream5 = KafkaStream.builder()
            .metadata(Metadata.builder()
                .name("stream2_test")
                .namespace("test")
                .cluster("local")
                .build())
            .build();

        when(streamRepository.findAllForCluster("local"))
                .thenReturn(List.of(stream1, stream2, stream3, stream4, stream5));

        KafkaStreamSearchParams params1 = KafkaStreamSearchParams.builder()
            .name(List.of("test_stream1", "test_stream2")).build();
        assertEquals(List.of(stream1, stream2), streamService.findAllForNamespace(ns, params1));

        KafkaStreamSearchParams params2 = KafkaStreamSearchParams.builder()
            .name(List.of("stream2_test", "test.stream10")).build();
        assertEquals(List.of(stream5), streamService.findAllForNamespace(ns, params2));

        KafkaStreamSearchParams params3 = KafkaStreamSearchParams.builder()
            .name(List.of("streamTEST", "TEST-stream3")).build();
        assertTrue(streamService.findAllForNamespace(ns, params3).isEmpty());

        KafkaStreamSearchParams params4 = KafkaStreamSearchParams.builder()
            .name(List.of("test_stream1", "test_stream1", "test_stream1")).build();
        assertEquals(List.of(stream1), streamService.findAllForNamespace(ns, params4));

        KafkaStreamSearchParams params5 = KafkaStreamSearchParams.builder()
            .name(List.of("test_*", "stream?_test")).build();
        assertEquals(List.of(stream1, stream2, stream3, stream5), streamService.findAllForNamespace(ns, params5));

        KafkaStreamSearchParams params6 = KafkaStreamSearchParams.builder()
            .name(List.of("test*", "stream??_test")).build();
        assertEquals(List.of(stream1, stream2, stream3, stream4), streamService.findAllForNamespace(ns, params6));

        KafkaStreamSearchParams params7 = KafkaStreamSearchParams.builder()
            .name(List.of("stream??_test", "kafka*stream*")).build();
        assertTrue(streamService.findAllForNamespace(ns, params7).isEmpty());

        KafkaStreamSearchParams params8 = KafkaStreamSearchParams.builder()
            .name(List.of("*_test", "*_test")).build();
        assertEquals(List.of(stream5), streamService.findAllForNamespace(ns, params8));

        KafkaStreamSearchParams params9 = KafkaStreamSearchParams.builder()
            .name(List.of("test_stream1", "test_stream3", "*_test")).build();
        assertEquals(List.of(stream1, stream3, stream5), streamService.findAllForNamespace(ns, params9));

        KafkaStreamSearchParams params10 = KafkaStreamSearchParams.builder()
            .name(List.of("test_stream9", "*_test")).build();
        assertEquals(List.of(stream5), streamService.findAllForNamespace(ns, params10));

        KafkaStreamSearchParams params11 = KafkaStreamSearchParams.builder()
            .name(List.of("*-dev", "stream_topic")).build();
        assertTrue(streamService.findAllForNamespace(ns, params11).isEmpty());

        KafkaStreamSearchParams params12 = KafkaStreamSearchParams.builder()
            .name(List.of("test_stream?", "test_stream1")).build();
        assertEquals(List.of(stream1, stream2, stream3), streamService.findAllForNamespace(ns, params12));
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

        when(accessControlEntryService.findAllGrantedToNamespace(ns))
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
