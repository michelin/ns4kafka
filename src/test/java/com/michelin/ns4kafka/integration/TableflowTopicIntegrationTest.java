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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.integration.TopicIntegrationTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.integration.container.SchemaRegistryIntegrationTest;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.Status;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import com.michelin.ns4kafka.service.executor.TableflowTopicAsyncExecutor;
import com.michelin.ns4kafka.validation.ResourceValidator;
import com.michelin.ns4kafka.validation.TableflowTopicValidator;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.BlockingHttpClient;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@MicronautTest
@Property(name = "ns4kafka.managed-clusters.test-cluster-confluent-cloud.manage-tableflow", value = "true")
class TableflowTopicIntegrationTest extends SchemaRegistryIntegrationTest {
    private static final String CONFLUENT_CLOUD_CLUSTER = "test-cluster-confluent-cloud";

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    @Client("/")
    private HttpClient ns4KafkaClient;

    @Inject
    private ConfluentCloudClient confluentCloudClient;

    @Inject
    private List<TableflowTopicAsyncExecutor> tableflowTopicAsyncExecutors;

    private HttpClient schemaRegistryClient;

    private String token;

    @MockBean(ConfluentCloudClient.class)
    ConfluentCloudClient confluentCloudClient() {
        return mock(ConfluentCloudClient.class);
    }

    @BeforeAll
    void init() {
        // Create HTTP client as bean to load client configuration from application.yml
        schemaRegistryClient = applicationContext.createBean(HttpClient.class, getSchemaRegistryUrl());

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        token = response.getBody().get().getAccessToken();

        Namespace ns1 = Namespace.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1")
                        .cluster(CONFLUENT_CLOUD_CLUSTER)
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .tableflowTopicValidator(TableflowTopicValidator.builder()
                                .validationConstraints(Map.of(
                                        "tableFormats", ResourceValidator.ValidList.in("ICEBERG"),
                                        "storage.kind", ResourceValidator.ValidString.in("Managed")))
                                .build())
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(ns1));

        AccessControlEntry acl = AccessControlEntry.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1-acl")
                        .namespace("ns1")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("ns1.")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("ns1")
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/acls")
                        .bearerAuth(token)
                        .body(acl));
    }

    @Test
    void shouldCreateTableflowTopic() {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1.create")
                        .namespace("ns1")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(3)
                        .replicationFactor(1)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "1", "retention.ms", "60000"))
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                        .bearerAuth(token)
                        .body(topic));

        schemaRegistryClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.POST, "/subjects/ns1.create-value/versions")
                                .body(
                                        Map.of(
                                                "schema",
                                                "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}")));

        TableflowTopicResponse tableflowTopicResponse = TableflowTopicResponse.builder()
                .id("tf-ns1.create")
                .spec(TableflowTopicResponse.Spec.builder()
                        .displayName("ns1.create")
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(new TableflowTopicResponse.Storage("Managed"))
                        .build())
                .status(new TableflowTopicResponse.Status("RUNNING", null))
                .build();

        when(confluentCloudClient.listTableflowTopics(CONFLUENT_CLOUD_CLUSTER)).thenReturn(Flux.empty());
        when(confluentCloudClient.createTableflowTopic(eq(CONFLUENT_CLOUD_CLUSTER), any()))
                .thenReturn(Mono.just(tableflowTopicResponse));

        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder().name("ns1.create").build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();

        HttpResponse<TableflowTopic> response = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/tableflow-topics")
                                .bearerAuth(token)
                                .body(tableflowTopic),
                        TableflowTopic.class);

        assertEquals("created", response.header("X-Ns4kafka-Result"));
        assertTrue(response.getBody().isPresent());
        assertEquals("ns1", response.getBody().get().getMetadata().getNamespace());
        assertEquals(
                CONFLUENT_CLOUD_CLUSTER, response.getBody().get().getMetadata().getCluster());
        assertEquals(
                Resource.Metadata.Phase.PENDING,
                response.getBody().get().getMetadata().getStatus().getPhase());
        assertEquals(Set.of("ICEBERG"), response.getBody().get().getSpec().getTableFormats());
        assertEquals("Managed", response.getBody().get().getSpec().getStorage().getKind());

        tableflowTopicAsyncExecutors.forEach(
                tableflowTopicAsyncExecutor -> tableflowTopicAsyncExecutor.run().block());

        HttpResponse<TableflowTopic> unchangedResponse = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/tableflow-topics")
                                .bearerAuth(token)
                                .body(tableflowTopic),
                        TableflowTopic.class);

        assertEquals("unchanged", unchangedResponse.header("X-Ns4kafka-Result"));
        assertTrue(unchangedResponse.getBody().isPresent());
        assertEquals(
                Resource.Metadata.Phase.SUCCESS,
                unchangedResponse.getBody().get().getMetadata().getStatus().getPhase());
    }

    @Test
    void shouldNotCreateTableflowTopicWhenSpecIsInvalid() {
        Topic topic = Topic.builder()
                .metadata(Resource.Metadata.builder()
                        .name("ns1.invalid")
                        .namespace("ns1")
                        .build())
                .spec(Topic.TopicSpec.builder()
                        .partitions(3)
                        .replicationFactor(1)
                        .configs(
                                Map.of("cleanup.policy", "delete", "min.insync.replicas", "1", "retention.ms", "60000"))
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/topics")
                        .bearerAuth(token)
                        .body(topic));

        TableflowTopic tableflowTopic = TableflowTopic.builder()
                .metadata(Resource.Metadata.builder().name("ns1.invalid").build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(Set.of("DELTA"))
                        .storage(TableflowTopic.Storage.builder().kind("BYOS").build())
                        .build())
                .build();

        HttpRequest<?> request = HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/tableflow-topics")
                .bearerAuth(token)
                .body(tableflowTopic);

        BlockingHttpClient blockingClient = ns4KafkaClient.toBlocking();

        HttpClientResponseException exception = assertThrows(
                HttpClientResponseException.class, () -> blockingClient.exchange(request, TableflowTopic.class));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Resource validation failed", exception.getMessage());
        assertTrue(exception.getResponse().getBody(Status.class).isPresent());

        List<String> causes =
                exception.getResponse().getBody(Status.class).get().getDetails().getCauses();

        assertEquals(2, causes.size());
        assertTrue(causes.contains(
                "Invalid value \"DELTA\" for field \"tableFormats\": value must be one of \"ICEBERG\"."));
        assertTrue(causes.contains(
                "Invalid value \"BYOS\" for field \"storage.kind\": value must be one of \"Managed\"."));
    }
}
