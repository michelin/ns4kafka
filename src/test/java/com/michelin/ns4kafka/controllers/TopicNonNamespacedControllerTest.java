package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.topic.TopicNonNamespacedController;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.services.TopicService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TopicNonNamespacedControllerTest {
    @Mock
    TopicService topicService;

    @InjectMocks
    TopicNonNamespacedController topicController;

    /**
     * Validate topics listing
     */
    @Test
    void listAll() {
        when(topicService.findAll())
                .thenReturn(List.of(
                        Topic.builder().metadata(ObjectMeta.builder().name("topic1").build()).build(),
                        Topic.builder().metadata(ObjectMeta.builder().name("topic2").build()).build()
                ));

        List<Topic> actual = topicController.listAll();

        Assertions.assertEquals(2, actual.size());
        Assertions.assertEquals("topic1", actual.get(0).getMetadata().getName());
        Assertions.assertEquals("topic2", actual.get(1).getMetadata().getName());
    }
}
