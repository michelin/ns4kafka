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

import com.michelin.ns4kafka.integration.container.KafkaIntegrationTest;
import com.michelin.ns4kafka.model.AccessControlEntry;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.model.Status;
import com.michelin.ns4kafka.model.Topic;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.type.Argument;
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
class NamespaceIntegrationTest extends KafkaIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    private String token;

    @BeforeAll
    void init() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), TopicIntegrationTest.BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        token = response.getBody().get().getAccessToken();
    }

    @Test
    void shouldValidateNamespaceNameWithAuthorizedChars() {
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("wrong*namespace")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-1"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("wrong_user")
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        HttpRequest<?> request = HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(namespace);

        BlockingHttpClient blockingClient = ns4KafkaClient.toBlocking();

        HttpClientResponseException exception =
                assertThrows(HttpClientResponseException.class, () -> blockingClient.exchange(request));

        assertEquals("Constraint validation failed", exception.getMessage());
        assertTrue(exception.getResponse().getBody(Status.class).isPresent());
        assertEquals(
                "namespace.metadata.name: must match \"^[a-zA-Z0-9_.-]+$\"",
                exception
                        .getResponse()
                        .getBody(Status.class)
                        .get()
                        .getDetails()
                        .getCauses()
                        .getFirst());

        namespace.getMetadata().setName("accepted.namespace");

        var responseCreateNs = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(namespace));

        assertEquals("created", responseCreateNs.header("X-Ns4kafka-Result"));

        var responseGetNs = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(HttpMethod.GET, "/api/namespaces?name=accepted.namespace")
                                .bearerAuth(token),
                        Namespace.class);

        assertEquals(namespace.getSpec(), responseGetNs.getSpec());

        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("accepted.namespace-rb")
                        .namespace("accepted.namespace")
                        .build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/accepted.namespace/role-bindings")
                        .bearerAuth(token)
                        .body(roleBinding));

        AccessControlEntry accessControlEntry = AccessControlEntry.builder()
                .metadata(Metadata.builder()
                        .name("accepted.namespace-acl")
                        .namespace("accepted.namespace")
                        .build())
                .spec(AccessControlEntry.AccessControlEntrySpec.builder()
                        .resourceType(AccessControlEntry.ResourceType.TOPIC)
                        .resource("accepted.namespace.")
                        .resourcePatternType(AccessControlEntry.ResourcePatternType.PREFIXED)
                        .permission(AccessControlEntry.Permission.OWNER)
                        .grantedTo("accepted.namespace")
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/accepted.namespace/acls")
                        .bearerAuth(token)
                        .body(accessControlEntry));

        Topic topic = Topic.builder()
                .metadata(Metadata.builder()
                        .name("accepted.namespace.topic")
                        .namespace("accepted.namespace")
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
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/accepted.namespace/topics")
                        .bearerAuth(token)
                        .body(topic));

        var responseGetTopic = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.create(
                                        HttpMethod.GET,
                                        "/api/namespaces/accepted.namespace/topics/accepted.namespace.topic")
                                .bearerAuth(token),
                        Topic.class);

        assertEquals(topic.getSpec(), responseGetTopic.getSpec());
    }

    @Test
    void shouldFailWhenRequestedNamespaceIsUnknown() {
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("namespace")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-1"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user")
                        .connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(
                        Metadata.builder().name("ns1-rb").namespace("namespace").build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(namespace));

        var response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespace/role-bindings")
                        .bearerAuth(token)
                        .body(roleBinding));

        assertEquals(HttpStatus.OK, response.getStatus());

        // Accessing unknown namespace
        HttpRequest<?> request = HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespaceTypo/role-bindings")
                .bearerAuth(token)
                .body(roleBinding);

        BlockingHttpClient blockingClient = ns4KafkaClient.toBlocking();

        HttpClientResponseException exception =
                assertThrows(HttpClientResponseException.class, () -> blockingClient.exchange(request));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Accessing unknown namespace \"namespaceTypo\"", exception.getMessage());
        assertTrue(exception.getResponse().getBody(Status.class).isPresent());
    }

    @Test
    void shouldFailWhenRequestedNamespaceIsForbidden() {
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("namespace2")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-1"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user2")
                        .connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("ns1-rb")
                        .namespace("namespace2")
                        .build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("group1")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(namespace));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespace2/role-bindings")
                        .bearerAuth(token)
                        .body(roleBinding));

        Namespace otherNamespace = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("namespace3")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-1"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user3")
                        .connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        RoleBinding otherRb = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("ns1-rb2")
                        .namespace("namespace3")
                        .build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("userGroup")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(otherNamespace));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespace3/role-bindings")
                        .bearerAuth(token)
                        .body(otherRb));

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("user", "admin");

        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), TopicIntegrationTest.BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        String userToken = response.getBody().get().getAccessToken();

        // Accessing forbidden namespace when only having access to namespace3
        HttpRequest<?> request = HttpRequest.create(HttpMethod.GET, "/api/namespaces/namespace2/topics")
                .bearerAuth(userToken)
                .body(roleBinding);

        BlockingHttpClient blockingClient = ns4KafkaClient.toBlocking();

        HttpClientResponseException exception =
                assertThrows(HttpClientResponseException.class, () -> blockingClient.exchange(request));

        assertEquals(HttpStatus.FORBIDDEN, exception.getStatus());
        assertEquals("Accessing forbidden namespace \"namespace2\"", exception.getMessage());
        assertTrue(exception.getResponse().getBody(Status.class).isPresent());
    }

    @Test
    void shouldIgnoreCaseOnGroupsAndAccessNamespace() {
        Namespace namespace = Namespace.builder()
                .metadata(Metadata.builder()
                        .name("namespace4")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-4"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user4")
                        .connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        RoleBinding roleBinding = RoleBinding.builder()
                .metadata(Metadata.builder()
                        .name("ns4-rb")
                        .namespace("namespace4")
                        .build())
                .spec(RoleBinding.RoleBindingSpec.builder()
                        .role(RoleBinding.Role.builder()
                                .resourceTypes(List.of("topics", "acls"))
                                .verbs(List.of(RoleBinding.Verb.POST, RoleBinding.Verb.GET))
                                .build())
                        .subject(RoleBinding.Subject.builder()
                                .subjectName("uSeRGROuP")
                                .subjectType(RoleBinding.SubjectType.GROUP)
                                .build())
                        .build())
                .build();

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                        .bearerAuth(token)
                        .body(namespace));

        ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/namespace4/role-bindings")
                        .bearerAuth(token)
                        .body(roleBinding));

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("user", "admin");

        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), TopicIntegrationTest.BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        String userToken = response.getBody().get().getAccessToken();

        HttpRequest<?> topicRequest = HttpRequest.create(HttpMethod.GET, "/api/namespaces/namespace4/topics")
                .bearerAuth(userToken)
                .body(roleBinding);

        HttpResponse<List<Topic>> topicResponse =
                ns4KafkaClient.toBlocking().exchange(topicRequest, Argument.listOf(Topic.class));

        assertEquals(HttpStatus.OK, topicResponse.getStatus());
    }
}
