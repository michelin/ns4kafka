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

import com.michelin.ns4kafka.integration.TopicIntegrationTest.BearerAccessRefreshToken;
import com.michelin.ns4kafka.integration.container.KafkaIntegrationTest;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.model.AccessControlEntry.Permission;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.model.AccessControlEntry.ResourceType;
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
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.BlockingHttpClient;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
class ExceptionHandlerIntegrationTest extends KafkaIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    private String token;

    @BeforeAll
    void init() {
        Namespace namespace = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .cluster("test-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        RoleBinding roleBinding = RoleBinding.builder()
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
                .body(namespace));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings")
                .bearerAuth(token)
                .body(roleBinding));

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

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Constraint validation failed", exception.getMessage());
        assertTrue(exception.getResponse().getBody(Status.class).isPresent());
        assertEquals("topic.metadata.name: must match \"^[a-zA-Z0-9_.-]+$\"",
            exception.getResponse().getBody(Status.class).get().getDetails().getCauses().getFirst());
    }

    @Test
    void shouldThrowUnknownNamespaceForTopic() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                    .create(HttpMethod.GET, "/api/namespaces/ns2/topics")
                    .bearerAuth(token)));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Accessing unknown namespace \"ns2\"", exception.getMessage());
    }

    @Test
    void shouldThrowUnauthorizedWhenNotAuthenticatedForTopic() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                    .create(HttpMethod.GET, "/api/namespaces/ns1/topics")));

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatus());
        assertEquals("Client '/': Unauthorized", exception.getMessage());
    }

    @Test
    void shouldThrowNamespaceForbiddenWhenAccessingForbiddenNamespaceAsUser() {
        Namespace ns = Namespace.builder()
            .metadata(Metadata.builder()
                .name("userNs")
                .cluster("test-cluster")
                .build())
            .spec(NamespaceSpec.builder()
                .kafkaUser("user2")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        RoleBinding rb = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("userNs-rb")
                .namespace("userNs")
                .build())
            .spec(RoleBindingSpec.builder()
                .role(Role.builder()
                    .resourceTypes(List.of("topics", "acls"))
                    .verbs(List.of(Verb.POST, Verb.GET))
                    .build())
                .subject(Subject.builder()
                    .subjectName("userGroup")
                    .subjectType(SubjectType.GROUP)
                    .build())
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(ns));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/userNs/role-bindings")
                .bearerAuth(token)
                .body(rb));

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("user", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .POST("/login", credentials), BearerAccessRefreshToken.class);

        String userToken = response.getBody().get().getAccessToken();

        // Trying to access ns1 when having no role binding for it
        HttpRequest<?> request = HttpRequest
            .create(HttpMethod.GET, "/api/namespaces/ns1/acls/ns2-acl")
            .bearerAuth(userToken);

        BlockingHttpClient blockingClient = ns4KafkaClient.toBlocking();

        HttpClientResponseException exception =
            assertThrows(HttpClientResponseException.class, () -> blockingClient.exchange(request));

        assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        assertEquals("Accessing forbidden namespace \"ns1\"", exception.getMessage());
    }

    @Test
    void shouldThrowResourceForbiddenWhenAccessingForbiddenResourceAsUser() {
        RoleBinding rb2 = RoleBinding.builder()
            .metadata(Metadata.builder()
                .name("ns1-rb2")
                .namespace("ns1")
                .build())
            .spec(RoleBindingSpec.builder()
                .role(Role.builder()
                    .resourceTypes(List.of("acls"))
                    .verbs(List.of(Verb.POST, Verb.GET))
                    .build())
                .subject(Subject.builder()
                    .subjectName("userGroup")
                    .subjectType(SubjectType.GROUP)
                    .build())
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/role-bindings")
                .bearerAuth(token)
                .body(rb2));

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("user", "admin");
        HttpResponse<BearerAccessRefreshToken> response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class);

        String userToken = response.getBody().get().getAccessToken();

        // Trying to access ns1 topics when the only role binding is for acls
        HttpRequest<?> request = HttpRequest
            .create(HttpMethod.GET, "/api/namespaces/ns1/topics")
            .bearerAuth(userToken);

        BlockingHttpClient blockingClient = ns4KafkaClient.toBlocking();

        HttpClientResponseException exception =
            assertThrows(HttpClientResponseException.class, () -> blockingClient.exchange(request));

        assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        assertEquals("Resource forbidden", exception.getMessage());
    }

    @Test
    void shouldNotFoundTopic() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                    .create(HttpMethod.GET, "/api/namespaces/ns1/topics/not-found-topic")
                    .bearerAuth(token)));

        assertEquals(HttpStatus.NOT_FOUND, exception.getStatus());
        assertEquals("Not Found", exception.getMessage());
    }

    @Test
    void shouldNotValidateUnknownHttpVerbForTopic() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest
                    .create(HttpMethod.PUT, "/api/namespaces/ns1/topics/")
                    .bearerAuth(token)));

        assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        assertEquals("Resource forbidden", exception.getMessage());
    }
}
