package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.validation.ConnectValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NamespaceTest {
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
        Namespace differentByConnectClusters = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace1")
                        .cluster("local")
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("connect1","connect2"))
                        .topicValidator(TopicValidator.makeDefault())
                        .connectValidator(ConnectValidator.makeDefault())
                        .build())
                .build();
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

        Assertions.assertEquals(original, same);

        Assertions.assertNotEquals(original, differentByMetadata);
        Assertions.assertNotEquals(original, differentByUser);
        Assertions.assertNotEquals(original, differentByConnectClusters);
        Assertions.assertNotEquals(original, differentByTopicValidator);
        Assertions.assertNotEquals(original, differentByConnectValidator);
    }
}
