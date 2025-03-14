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
package com.michelin.ns4kafka.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import java.util.List;
import org.junit.jupiter.api.Test;

class NamespaceTest {
    @Test
    void shouldBeEqual() {
        Namespace original = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("connect1"))
                        .topicValidator(TopicValidator.makeDefault())
                        .connectValidator(ConnectValidator.makeDefault())
                        .build())
                .build();
        Namespace same = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("connect1"))
                        .topicValidator(TopicValidator.makeDefault())
                        .connectValidator(ConnectValidator.makeDefault())
                        .build())
                .build();
        Namespace differentByMetadata = Namespace.builder()
                .metadata(Metadata.builder().name("namespace2").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("connect1"))
                        .topicValidator(TopicValidator.makeDefault())
                        .connectValidator(ConnectValidator.makeDefault())
                        .build())
                .build();

        assertEquals(original, same);

        assertNotEquals(original, differentByMetadata);

        Namespace differentByUser = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user2")
                        .connectClusters(List.of("connect1"))
                        .topicValidator(TopicValidator.makeDefault())
                        .connectValidator(ConnectValidator.makeDefault())
                        .build())
                .build();

        assertNotEquals(original, differentByUser);

        Namespace differentByConnectClusters = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("connect1", "connect2"))
                        .topicValidator(TopicValidator.makeDefault())
                        .connectValidator(ConnectValidator.makeDefault())
                        .build())
                .build();

        assertNotEquals(original, differentByConnectClusters);

        Namespace differentByTopicValidator = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("connect1"))
                        .topicValidator(TopicValidator.builder().build())
                        .connectValidator(ConnectValidator.makeDefault())
                        .build())
                .build();

        assertNotEquals(original, differentByTopicValidator);

        Namespace differentByConnectValidator = Namespace.builder()
                .metadata(Metadata.builder().name("namespace1").cluster("local").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("connect1"))
                        .topicValidator(TopicValidator.makeDefault())
                        .connectValidator(ConnectValidator.builder().build())
                        .build())
                .build();

        assertNotEquals(original, differentByConnectValidator);
    }
}
