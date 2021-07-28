package com.michelin.ns4kafka.services;

import static org.mockito.Mockito.when;

import java.util.List;

import com.michelin.ns4kafka.models.KafkaStream;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.repositories.StreamRepository;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamServiceTest {

    @InjectMocks
    StreamService streamService;

    @Mock
    AccessControlEntryService accessControlEntryService;

    @Mock
    StreamRepository streamRepository;

    @Test
    void findAllEmpty() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();


        when(streamRepository.findAllForCluster("local"))
                .thenReturn(List.of());
        var actual = streamService.findAllForNamespace(ns);
        Assertions.assertTrue(actual.isEmpty());
    }

    @Test
    void findAll() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .namespace("test")
                      .cluster("local")
                      .build())
            .build();
        KafkaStream stream2 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream2")
                      .namespace("test")
                      .cluster("local")
                      .build())
            .build();
        KafkaStream stream3 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream3")
                      .namespace("test")
                      .cluster("local")
                      .build())
            .build();


        when(streamRepository.findAllForCluster("local"))
                .thenReturn(List.of(stream1, stream2, stream3));
        var actual = streamService.findAllForNamespace(ns);
        Assertions.assertEquals(3,actual.size());
        Assertions.assertTrue(actual.contains(stream1));
        Assertions.assertTrue(actual.contains(stream2));
        Assertions.assertTrue(actual.contains(stream3));
    }

    @Test
    void findByName() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();
        KafkaStream stream1 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream1")
                      .namespace("test")
                      .cluster("local")
                      .build())
            .build();
        KafkaStream stream2 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream2")
                      .namespace("test")
                      .cluster("local")
                      .build())
            .build();
        KafkaStream stream3 = KafkaStream.builder()
            .metadata(ObjectMeta.builder()
                      .name("test_stream3")
                      .namespace("test")
                      .cluster("local")
                      .build())
            .build();


        when(streamRepository.findAllForCluster("local"))
                .thenReturn(List.of(stream1, stream2, stream3));
        var actual = streamService.findByName(ns, "test_stream2");
        Assertions.assertTrue(actual.isPresent());
        Assertions.assertEquals(stream2, actual.get());
    }

    @Test
    void findByNameEmpty() {

        Namespace ns = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("test")
                        .cluster("local")
                        .build())
                .build();

        when(streamRepository.findAllForCluster("local"))
                .thenReturn(List.of());
        var actual = streamService.findByName(ns, "test_stream2");
        Assertions.assertTrue(actual.isEmpty());
    }
}
