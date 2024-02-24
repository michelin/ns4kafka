package com.michelin.ns4kafka.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controllers.topic.TopicNonNamespacedController;
import com.michelin.ns4kafka.models.Metadata;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.services.TopicService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicNonNamespacedControllerTest {
    @Mock
    TopicService topicService;

    @InjectMocks
    TopicNonNamespacedController topicController;

    @Test
    void listAll() {
        when(topicService.findAll())
            .thenReturn(List.of(
                Topic.builder().metadata(Metadata.builder().name("topic1").build()).build(),
                Topic.builder().metadata(Metadata.builder().name("topic2").build()).build()
            ));

        List<Topic> actual = topicController.listAll();

        assertEquals(2, actual.size());
        assertEquals("topic1", actual.get(0).getMetadata().getName());
        assertEquals("topic2", actual.get(1).getMetadata().getName());
    }
}
