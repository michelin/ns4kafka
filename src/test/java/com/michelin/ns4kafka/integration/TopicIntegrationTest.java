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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.controller.AkhqClaimProviderController;
import com.michelin.ns4kafka.integration.container.KafkaIntegrationTest;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.model.AccessControlEntry.Permission;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.model.DeleteRecordsResponse;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.RoleBinding.Role;
import com.michelin.ns4kafka.model.RoleBinding.RoleBindingSpec;
import com.michelin.ns4kafka.model.RoleBinding.Subject;
import com.michelin.ns4kafka.model.RoleBinding.SubjectType;
import com.michelin.ns4kafka.model.RoleBinding.Verb;
import com.michelin.ns4kafka.model.Status;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.model.Topic.TopicSpec;
import com.michelin.ns4kafka.service.executor.TopicAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
class TopicIntegrationTest extends KafkaIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    @Inject
    List<TopicAsyncExecutor> topicAsyncExecutorList;

    private String token;

    @BeforeAll
    void init() {
        Namespace ns1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-1"))
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        RoleBinding rb1 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb")
                .namespace("ns1")
                .build())
            .spec(RoleBindingSpec.builder()
                .role(Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(Verb.POST, Verb.GET))
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
            .exchange(HttpRequest
                .POST("/login", credentials), BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(ns1));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings")
                .bearerAuth(token)
                .body(rb1));

        AccessControlEntry ns1acl = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acl")
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

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(ns1acl));

        Namespace ns2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns2")
                .cluster("test-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user2")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(ns2));

        RoleBinding rb2 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns2-rb")
                .namespace("ns2")
                .build())
            .spec(RoleBindingSpec.builder()
                .role(Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(Verb.POST, Verb.GET))
                    .build())
                .subject(Subject.builder()
                    .subjectName("group2")
                    .subjectType(SubjectType.GROUP)
                    .build())
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns2/role-bindings")
                .bearerAuth(token)
                .body(rb2));

        AccessControlEntry ns2acl = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns2-acl")
                .namespace("ns2")
                .build())
            .spec(AccessControlEntrySpec.builder()
                .resourceType(ResourceType.TOPIC)
                .resource("ns2-")
                .resourcePatternType(ResourcePatternType.PREFIXED)
                .permission(Permission.OWNER)
                .grantedTo("ns2")
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns2/acls")
                .bearerAuth(token)
                .body(ns2acl));
    }

    @Test
    void shouldValidateAkhqClaims() {
        AkhqClaimProviderController.AkhqClaimRequest akhqClaimRequest =
            AkhqClaimProviderController.AkhqClaimRequest.builder()
                .username("test")
                .groups(List.of("LDAP-GROUP-1"))
                .providerName("LDAP")
                .build();

        AkhqClaimProviderController.AkhqClaimResponse response = ns4KafkaClient
            .toBlocking()
            .retrieve(HttpRequest
                .POST("/akhq-claim", akhqClaimRequest), AkhqClaimProviderController.AkhqClaimResponse.class);

        assertLinesMatch(
            List.of(
                "topic/read",
                "topic/data/read",
                "group/read",
                "registry/read",
                "connect/read",
                "connect/state/update"
            ),
            response.getRoles());

        assertEquals(1, response.getAttributes().get("topicsFilterRegexp").size());

        assertLinesMatch(List.of("^\\Qns1-\\E.*$"), response.getAttributes().get("topicsFilterRegexp"));
    }

    @Test
    void shouldCreateTopic() throws InterruptedException, ExecutionException {
        Topic topicFirstCreate = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-topicFirstCreate")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        var response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topicFirstCreate));

        assertEquals("created", response.header("X-Ns4kafka-Result"));

        forceTopicSynchronization();

        Admin kafkaClient = getAdminClient();

        List<TopicPartitionInfo> topicPartitionInfos = kafkaClient
            .describeTopics(List.of("ns1-topicFirstCreate"))
            .allTopicNames()
            .get()
            .get("ns1-topicFirstCreate")
            .partitions();

        assertEquals(topicFirstCreate.getSpec().getPartitions(), topicPartitionInfos.size());

        Map<String, String> config = topicFirstCreate.getSpec().getConfigs();
        Set<String> configKey = config.keySet();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "ns1-topicFirstCreate");
        List<ConfigEntry> valueToVerify = kafkaClient
            .describeConfigs(List.of(configResource))
            .all()
            .get()
            .get(configResource)
            .entries()
            .stream()
            .filter(e -> configKey.contains(e.name()))
            .toList();

        assertEquals(config.size(), valueToVerify.size());
        valueToVerify.forEach(entry -> assertEquals(config.get(entry.name()), entry.value()));
    }

    @Test
    void shouldUpdateTopic() throws InterruptedException, ExecutionException {
        Topic topic = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-topic2Create")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        var response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topic));

        assertEquals("created", response.header("X-Ns4kafka-Result"));

        forceTopicSynchronization();

        response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topic));

        assertEquals("unchanged", response.header("X-Ns4kafka-Result"));

        Topic topicToUpdate = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-topic2Create")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of(
                    "cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "70000")) //This line was changed
                .build())
            .build();

        response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topicToUpdate));

        assertEquals("changed", response.header("X-Ns4kafka-Result"));

        forceTopicSynchronization();

        Admin kafkaClient = getAdminClient();

        List<TopicPartitionInfo> topicPartitionInfos = kafkaClient
            .describeTopics(List.of("ns1-topic2Create"))
            .allTopicNames()
            .get()
            .get("ns1-topic2Create")
            .partitions();

        // verify partition of the updated topic
        assertEquals(topicToUpdate.getSpec().getPartitions(), topicPartitionInfos.size());

        // verify config of the updated topic
        Map<String, String> config = topicToUpdate.getSpec().getConfigs();
        Set<String> configKey = config.keySet();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "ns1-topic2Create");
        List<ConfigEntry> valueToVerify = kafkaClient
            .describeConfigs(List.of(configResource))
            .all()
            .get()
            .get(configResource)
            .entries()
            .stream()
            .filter(e -> configKey.contains(e.name()))
            .toList();

        assertEquals(config.size(), valueToVerify.size());
        valueToVerify.forEach(entry -> assertEquals(config.get(entry.name()), entry.value()));
    }

    @Test
    void shouldInvalidateTopicName() {
        Topic topicFirstCreate = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-invalid-é")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                    .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                    .bearerAuth(token)
                    .body(topicFirstCreate)));

        assertEquals("Constraint validation failed", exception.getMessage());
        assertTrue(exception.getResponse().getBody(Status.class).isPresent());
        assertEquals("topic.metadata.name: must match \"^[a-zA-Z0-9_.-]+$\"",
            exception.getResponse().getBody(Status.class).get().getDetails().getCauses().getFirst());
    }

    @Test
    void shouldUpdateTopicWithNoChange() {
        AccessControlEntry aclns1Tons2 = AccessControlEntry.builder()
            .metadata(Metadata.builder()
                .name("ns1-acltons2")
                .namespace("ns1")
                .build())
            .spec(AccessControlEntrySpec.builder()
                .resourceType(ResourceType.TOPIC)
                .resource("ns1-")
                .resourcePatternType(ResourcePatternType.PREFIXED)
                .permission(Permission.READ)
                .grantedTo("ns2")
                .build())
            .build();

        Topic topicToModify = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-topicToModify")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of(
                    "cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                .bearerAuth(token)
                .body(aclns1Tons2));

        assertEquals(HttpStatus.OK, ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topicToModify))
            .getStatus());

        Topic topicToModifyBis = Topic.builder()
            .metadata(topicToModify.getMetadata())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of(
                    "cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "90000"))
                .build())
            .build();

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                    .create(HttpMethod.POST, "/api/namespaces/ns2/topics")
                    .bearerAuth(token)
                    .body(topicToModifyBis)));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());

        // Compare spec of the topics and assure there is no change
        assertEquals(topicToModify.getSpec(), ns4KafkaClient
            .toBlocking()
            .retrieve(HttpRequest
                .create(HttpMethod.GET, "/api/namespaces/ns1/topics/ns1-topicToModify")
                .bearerAuth(token), Topic.class)
            .getSpec());
    }

    @Test
    void shouldDeleteRecords() throws InterruptedException {
        Topic topicToDelete = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-topicToDelete")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        var response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topicToDelete));

        assertEquals("created", response.header("X-Ns4kafka-Result"));

        forceTopicSynchronization();

        List<DeleteRecordsResponse> deleteRecordsResponse = ns4KafkaClient
            .toBlocking()
            .retrieve(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics/ns1-topicToDelete/delete-records")
                .bearerAuth(token), Argument.listOf(DeleteRecordsResponse.class));

        DeleteRecordsResponse resultPartition0 = deleteRecordsResponse
            .stream()
            .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 0)
            .findFirst()
            .orElse(null);

        assertEquals(3L, deleteRecordsResponse.size());

        assertNotNull(resultPartition0);
        assertEquals(0, resultPartition0.getSpec().getOffset());
        assertEquals(0, resultPartition0.getSpec().getPartition());
        assertEquals("ns1-topicToDelete", resultPartition0.getSpec().getTopic());

        DeleteRecordsResponse resultPartition1 = deleteRecordsResponse
            .stream()
            .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 1)
            .findFirst()
            .orElse(null);

        assertNotNull(resultPartition1);
        assertEquals(0, resultPartition1.getSpec().getOffset());
        assertEquals(1, resultPartition1.getSpec().getPartition());
        assertEquals("ns1-topicToDelete", resultPartition1.getSpec().getTopic());

        DeleteRecordsResponse resultPartition2 = deleteRecordsResponse
            .stream()
            .filter(deleteRecord -> deleteRecord.getSpec().getPartition() == 2)
            .findFirst()
            .orElse(null);

        assertNotNull(resultPartition2);
        assertEquals(0, resultPartition2.getSpec().getOffset());
        assertEquals(2, resultPartition2.getSpec().getPartition());
        assertEquals("ns1-topicToDelete", resultPartition2.getSpec().getTopic());
    }

    @Test
    void shouldDeleteRecordsOnCompactTopic() throws InterruptedException {
        Topic topicToDelete = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-compactTopicToDelete")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of(
                    "cleanup.policy", "compact",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        var response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topicToDelete));

        assertEquals("created", response.header("X-Ns4kafka-Result"));

        forceTopicSynchronization();

        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                    .create(HttpMethod.POST, "/api/namespaces/ns1/topics/compactTopicToDelete/delete-records")
                    .bearerAuth(token)));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
    }

    @Test
    void shouldDeleteTopics() throws InterruptedException, ExecutionException {
        Topic deleteTopic = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-deleteTopic")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "delete",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic compactTopic = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-compactTopic")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        Topic topicNotToDelete = Topic.builder()
            .metadata(Metadata.builder()
                .name("ns1-test")
                .namespace("ns1")
                .build())
            .spec(TopicSpec.builder()
                .partitions(3)
                .replicationFactor(1)
                .configs(Map.of("cleanup.policy", "compact",
                    "min.insync.replicas", "1",
                    "retention.ms", "60000"))
                .build())
            .build();

        var createResponse1 = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(deleteTopic));

        assertEquals("created", createResponse1.header("X-Ns4kafka-Result"));

        var createResponse2 = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(compactTopic));

        assertEquals("created", createResponse2.header("X-Ns4kafka-Result"));

        var createResponse3 = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                .bearerAuth(token)
                .body(topicNotToDelete));

        assertEquals("created", createResponse3.header("X-Ns4kafka-Result"));

        forceTopicSynchronization();

        var deleteResponse = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.DELETE, "/api/namespaces/ns1/topics?name=ns1-*Topic")
                .bearerAuth(token));

        assertEquals(HttpStatus.OK, deleteResponse.getStatus());

        forceTopicSynchronization();

        Admin kafkaClient = getAdminClient();

        Map<String, TopicListing> topics = kafkaClient.listTopics().namesToListings().get();

        assertFalse(topics.containsKey("ns1-deleteTopic"));
        assertFalse(topics.containsKey("ns1-compactTopic"));
        assertTrue(topics.containsKey("ns1-test"));
    }

    private void forceTopicSynchronization() throws InterruptedException {
        topicAsyncExecutorList.forEach(TopicAsyncExecutor::run);

        // Wait for topics to be updated in Kafka broker
        Thread.sleep(2000);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BearerAccessRefreshToken {
        private String username;
        private Collection<String> roles;
        @JsonProperty("access_token")
        private String accessToken;
        @JsonProperty("token_type")
        private String tokenType;
        @JsonProperty("expires_in")
        private Integer expiresIn;
    }
}
