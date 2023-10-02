package com.michelin.ns4kafka.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import java.util.List;
import org.junit.jupiter.api.Test;

class NamespaceTest {
    @Test
    void testEquals() {
        Namespace original = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("namespace1")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("connect1"))
                .topicValidator(TopicValidator.makeDefault())
                .connectValidator(ConnectValidator.makeDefault())
                .build())
            .build();
        Namespace same = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("namespace1")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("connect1"))
                .topicValidator(TopicValidator.makeDefault())
                .connectValidator(ConnectValidator.makeDefault())
                .build())
            .build();
        Namespace differentByMetadata = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("namespace2")
                .cluster("local")
                .build())
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
            .metadata(ObjectMeta.builder()
                .name("namespace1")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user2")
                .connectClusters(List.of("connect1"))
                .topicValidator(TopicValidator.makeDefault())
                .connectValidator(ConnectValidator.makeDefault())
                .build())
            .build();

        assertNotEquals(original, differentByUser);

        Namespace differentByConnectClusters = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("namespace1")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("connect1", "connect2"))
                .topicValidator(TopicValidator.makeDefault())
                .connectValidator(ConnectValidator.makeDefault())
                .build())
            .build();

        assertNotEquals(original, differentByConnectClusters);

        Namespace differentByTopicValidator = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("namespace1")
                .cluster("local")
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("connect1"))
                .topicValidator(TopicValidator.builder().build())
                .connectValidator(ConnectValidator.makeDefault())
                .build())
            .build();

        assertNotEquals(original, differentByTopicValidator);

        Namespace differentByConnectValidator = Namespace.builder()
            .metadata(ObjectMeta.builder()
                .name("namespace1")
                .cluster("local")
                .build())
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
