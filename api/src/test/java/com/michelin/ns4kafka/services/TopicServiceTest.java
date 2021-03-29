package com.michelin.ns4kafka.services;

import static org.mockito.Mockito.when;

import java.util.List;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TopicServiceTest {

    @Mock
    TopicRepository topicRepository;

    @InjectMocks
    TopicService topicService;


    Namespace ns = Namespace.builder()
        .metadata(ObjectMeta.builder()
                  .name("test")
                  .cluster("local")
                  .build())
        .build();

    @Test
    void findAllForNamespaceEmptyTopic(){
        when(topicRepository.findAllForCluster(ns.getMetadata().getCluster()))
            .thenReturn(List.of());
        List<Topic> actual = topicService.findAllForNamespace(ns);
        Assertions.assertTrue(actual.isEmpty());
    }
}
