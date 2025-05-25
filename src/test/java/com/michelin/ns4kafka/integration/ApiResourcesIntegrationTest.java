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

import com.michelin.ns4kafka.controller.ApiResourcesController;
import com.michelin.ns4kafka.integration.container.KafkaIntegrationTest;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.RoleBinding;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Test;

@MicronautTest
class ApiResourcesIntegrationTest extends KafkaIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    @Test
    void shouldListResourcesAsAdmin() {
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), TopicIntegrationTest.BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        String token = response.getBody().get().getAccessToken();

        List<ApiResourcesController.ResourceDefinition> resources = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.GET("/api-resources").bearerAuth(token),
                        Argument.listOf(ApiResourcesController.ResourceDefinition.class));

        assertEquals(9, resources.size());
    }

    @Test
    void shouldListResourcesAsAnonymous() {
        // This feature is not about restricting access, but easing user experience within the CLI
        // If the user is not authenticated, show everything
        List<ApiResourcesController.ResourceDefinition> resources = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.GET("/api-resources"),
                        Argument.listOf(ApiResourcesController.ResourceDefinition.class));

        assertEquals(9, resources.size());
    }

    @Test
    void shouldListResourcesAsUser() {
        Namespace ns1 = Namespace.builder()
                .metadata(Metadata.builder().name("ns1").cluster("test-cluster").build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        RoleBinding rb1 = RoleBinding.builder()
                .metadata(Metadata.builder().name("ns1-rb").namespace("ns1").build())
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

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response = ns4KafkaClient
                .toBlocking()
                .exchange(HttpRequest.POST("/login", credentials), TopicIntegrationTest.BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        String token = response.getBody().get().getAccessToken();

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

        UsernamePasswordCredentials userCredentials = new UsernamePasswordCredentials("user", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> userResponse = ns4KafkaClient
                .toBlocking()
                .exchange(
                        HttpRequest.POST("/login", userCredentials),
                        TopicIntegrationTest.BearerAccessRefreshToken.class);

        assertTrue(response.getBody().isPresent());

        String userToken = userResponse.getBody().get().getAccessToken();

        List<ApiResourcesController.ResourceDefinition> resources = ns4KafkaClient
                .toBlocking()
                .retrieve(
                        HttpRequest.GET("/api-resources").bearerAuth(userToken),
                        Argument.listOf(ApiResourcesController.ResourceDefinition.class));

        assertEquals(2, resources.size());
    }
}
