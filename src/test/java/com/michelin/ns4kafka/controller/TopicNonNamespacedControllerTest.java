/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.ns4kafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.controller.topic.TopicNonNamespacedController;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.service.TopicService;
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
    void shouldFindAll() {
        when(topicService.findAll())
            .thenReturn(List.of(
                Topic.builder()
                    .metadata(Metadata.builder()
                        .name("topic1")
                        .build())
                    .build(),
                Topic.builder()
                    .metadata(Metadata.builder()
                        .name("topic2")
                        .build())
                    .build()
            ));

        List<Topic> actual = topicController.listAll();

        assertEquals(2, actual.size());
        assertEquals("topic1", actual.get(0).getMetadata().getName());
        assertEquals("topic2", actual.get(1).getMetadata().getName());
    }
}
