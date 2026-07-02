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
package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.integration.TopicIntegrationTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.integration.container.KafkaIntegrationTest;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.model.AccessControlEntry.Permission;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.RoleBinding.Role;
import com.michelin.ns4kafka.model.RoleBinding.RoleBindingSpec;
import com.michelin.ns4kafka.model.RoleBinding.Subject;
import com.michelin.ns4kafka.model.RoleBinding.SubjectType;
import com.michelin.ns4kafka.model.RoleBinding.Verb;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroup;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroup.ConsumerGroupStatus;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets.ConsumerGroupResetOffsetsSpec;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsets.ResetOffsetsMethod;
import com.michelin.ns4kafka.model.consumer.group.ConsumerGroupResetOffsetsResponse;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
class ConsumerGroupIntegrationTest extends KafkaIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    private String token;

    @BeforeAll
    void init() {
        Namespace ns1 = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-1"))
                        .build())
                .spec(NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        RoleBinding rb1 = RoleBinding.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-rb")
                        .namespace("ns1")
                        .build())
                .spec(RoleBindingSpec.builder()
                        .role(Role.builder()
                                .resourceTypes(List.of("topics", "acls", "consumer-groups"))
                                .verbs(List.of(Verb.POST, Verb.GET, Verb.DELETE))
                                .build())
                        .subject(Subject.builder()
                                .subjectName("group1")
                                .subjectType(SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        token = response.getBody().get().getAccessToken();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(ns1));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings")
                        .bearerAuth(token)
                        .body(rb1));

        AccessControlEntry ns1TopicAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-topic-acl")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntrySpec.builder()
                        .resourceType(ResourceType.TOPIC)
                        .resource("ns1-")
                        .resourcePatternType(ResourcePatternType.PREFIXED)
                        .permission(Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        AccessControlEntry ns1GroupAcl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-group-acl")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntrySpec.builder()
                        .resourceType(ResourceType.GROUP)
                        .resource("ns1-")
                        .resourcePatternType(ResourcePatternType.PREFIXED)
                        .permission(Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                        .bearerAuth(token)
                        .body(ns1TopicAcl));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                        .bearerAuth(token)
                        .body(ns1GroupAcl));
    }

    @Test
    void shouldListConsumerGroups() throws ExecutionException, InterruptedException {
        createTopics(
                getAdminClient(), new NewTopic("ns1-topic", 3, (short) 1), new NewTopic("other-topic", 3, (short) 1));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaClientProperties(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())))) {
            producer.send(new ProducerRecord<>("ns1-topic", 0, "key0", "value0"))
                    .get();
            producer.send(new ProducerRecord<>("ns1-topic", 1, "key1", "value1"))
                    .get();
            producer.send(new ProducerRecord<>("ns1-topic", 2, "key2", "value2"))
                    .get();
            producer.send(new ProducerRecord<>("other-topic", 0, "key3", "value3"))
                    .get();
            producer.send(new ProducerRecord<>("other-topic", 1, "key4", "value4"))
                    .get();
            producer.send(new ProducerRecord<>("other-topic", 2, "key5", "value5"))
                    .get();
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaClientProperties(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG,
                "ns1-consumerGroup",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName())))) {
            consumer.subscribe(List.of("ns1-topic", "other-topic"));
            consumer.poll(Duration.ofSeconds(5));
            consumer.commitSync();
        }

        try (KafkaConsumer<String, String> outOfNamespaceConsumer = new KafkaConsumer<>(kafkaClientProperties(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG,
                "other-consumerGroup",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName())))) {
            outOfNamespaceConsumer.subscribe(List.of("ns1-topic", "other-topic"));
            outOfNamespaceConsumer.poll(Duration.ofSeconds(5));
            outOfNamespaceConsumer.commitSync();
        }

        List<ConsumerGroup> consumerGroups = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(HttpMethod.GET, "/api/namespaces/ns1/consumer-groups")
                                .bearerAuth(token),
                        Argument.listOf(ConsumerGroup.class));

        // The consumer group is flattened into one entry per topic-partition
        assertEquals(6, consumerGroups.size());
        assertTrue(consumerGroups.stream()
                .allMatch(consumerGroup ->
                        "ns1-consumerGroup".equals(consumerGroup.getMetadata().getName())));
        assertEquals(
                List.of(
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-topic", 0, 1L, 1L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-topic", 1, 1L, 1L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-topic", 2, 1L, 1L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "other-topic", 0, 1L, 1L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "other-topic", 1, 1L, 1L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "other-topic", 2, 1L, 1L, 0L)),
                consumerGroups.stream().map(ConsumerGroup::getStatus).toList());

        assertTrue(consumerGroups.stream()
                .noneMatch(consumerGroup ->
                        "other-consumerGroup".equals(consumerGroup.getMetadata().getName())));
    }

    @Test
    void shouldListExternalConsumerGroups() throws ExecutionException, InterruptedException {
        createTopics(
                getAdminClient(),
                new NewTopic("ns1-externalTopic", 3, (short) 1),
                new NewTopic("other-externalTopic", 3, (short) 1));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaClientProperties(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())))) {
            producer.send(new ProducerRecord<>("ns1-externalTopic", 0, "key0", "value0"))
                    .get();
            producer.send(new ProducerRecord<>("ns1-externalTopic", 1, "key1", "value1"))
                    .get();
            producer.send(new ProducerRecord<>("ns1-externalTopic", 2, "key2", "value2"))
                    .get();
            producer.send(new ProducerRecord<>("other-externalTopic", 0, "key3", "value3"))
                    .get();
            producer.send(new ProducerRecord<>("other-externalTopic", 1, "key4", "value4"))
                    .get();
            producer.send(new ProducerRecord<>("other-externalTopic", 2, "key5", "value5"))
                    .get();
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaClientProperties(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG,
                "externalConsumerGroup",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName())))) {
            consumer.subscribe(List.of("ns1-externalTopic", "other-externalTopic"));
            consumer.poll(Duration.ofSeconds(5));
            consumer.commitSync();
        }

        try (KafkaConsumer<String, String> outOfScopeConsumer = new KafkaConsumer<>(kafkaClientProperties(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG,
                "otherConsumerGroup",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName())))) {
            outOfScopeConsumer.subscribe(List.of("other-externalTopic"));
            outOfScopeConsumer.poll(Duration.ofSeconds(5));
            outOfScopeConsumer.commitSync();
        }

        List<ConsumerGroup> externalConsumerGroups = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(HttpMethod.GET, "/api/namespaces/ns1/consumer-groups/_/external")
                                .bearerAuth(token),
                        Argument.listOf(ConsumerGroup.class));

        assertTrue(externalConsumerGroups.stream()
                .anyMatch(consumerGroup -> "externalConsumerGroup"
                        .equals(consumerGroup.getMetadata().getName())));
        List<ConsumerGroup> externalConsumerGroup = externalConsumerGroups.stream()
                .filter(consumerGroup -> "externalConsumerGroup"
                        .equals(consumerGroup.getMetadata().getName()))
                .toList();
        assertEquals(
                List.of(
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-externalTopic", 0, 1L, 1L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-externalTopic", 1, 1L, 1L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-externalTopic", 2, 1L, 1L, 0L)),
                externalConsumerGroup.stream().map(ConsumerGroup::getStatus).toList());

        assertTrue(externalConsumerGroups.stream()
                .noneMatch(consumerGroup ->
                        "otherConsumerGroup".equals(consumerGroup.getMetadata().getName())));
    }

    @Test
    void shouldResetOffsetsThenDeleteConsumerGroup() throws ExecutionException, InterruptedException {
        createTopics(getAdminClient(), new NewTopic("ns1-resetTopic", 3, (short) 1));

        ConsumerGroupResetOffsets resetOffsets = ConsumerGroupResetOffsets.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-resetConsumerGroup")
                        .namespace("ns1")
                        .build())
                .spec(ConsumerGroupResetOffsetsSpec.builder()
                        .topic("ns1-resetTopic")
                        .method(ResetOffsetsMethod.TO_EARLIEST)
                        .build())
                .build();

        List<ConsumerGroupResetOffsetsResponse> resetOffsetsResponses = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(
                                        HttpMethod.POST,
                                        "/api/namespaces/ns1/consumer-groups/ns1-resetConsumerGroup/reset")
                                .bearerAuth(token)
                                .body(resetOffsets),
                        Argument.listOf(ConsumerGroupResetOffsetsResponse.class));

        assertEquals(3, resetOffsetsResponses.size());
        assertTrue(resetOffsetsResponses.stream()
                .allMatch(response -> response.getSpec().getOffset() == 0));

        List<ConsumerGroup> consumerGroups = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(
                                        HttpMethod.GET,
                                        "/api/namespaces/ns1/consumer-groups?name=ns1-resetConsumerGroup")
                                .bearerAuth(token),
                        Argument.listOf(ConsumerGroup.class));

        assertEquals(3, consumerGroups.size());
        assertTrue(consumerGroups.stream()
                .allMatch(consumerGroup -> "ns1-resetConsumerGroup"
                        .equals(consumerGroup.getMetadata().getName())));
        assertEquals(
                List.of(
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-resetTopic", 0, 0L, 0L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-resetTopic", 1, 0L, 0L, 0L),
                        new ConsumerGroupStatus(GroupState.EMPTY, "ns1-resetTopic", 2, 0L, 0L, 0L)),
                consumerGroups.stream().map(ConsumerGroup::getStatus).toList());

        HttpResponse<Void> deleteResponse = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(
                                HttpMethod.DELETE, "/api/namespaces/ns1/consumer-groups/ns1-resetConsumerGroup")
                        .bearerAuth(token));

        assertEquals(HttpStatus.NO_CONTENT, deleteResponse.getStatus());

        consumerGroups = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(
                                        HttpMethod.GET,
                                        "/api/namespaces/ns1/consumer-groups?name=ns1-resetConsumerGroup")
                                .bearerAuth(token),
                        Argument.listOf(ConsumerGroup.class));

        assertTrue(consumerGroups.isEmpty());
    }

    /**
     * Build Kafka client properties.
     *
     * @param additionalProperties The client-specific properties to add
     * @return The Kafka client properties
     */
    private Map<String, Object> kafkaClientProperties(Map<String, Object> additionalProperties) {
        Map<String, Object> properties = new HashMap<>(Map.of(
                "bootstrap.servers",
                broker.getBootstrapServers(),
                "sasl.mechanism",
                "PLAIN",
                "security.protocol",
                "SASL_PLAINTEXT",
                "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule "
                        + "required username=\"admin\" password=\"admin\";"));
        properties.putAll(additionalProperties);
        return properties;
    }
}
