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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.integration.container.KafkaIntegrationTest;
import com.michelin.ns4kafka.model.KafkaUserResetPassword;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Status;
import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.service.executor.UserAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
class UserIntegrationTest extends KafkaIntegrationTest {
    @Inject
    @Client("/")
    HttpClient ns4KafkaClient;

    @Inject
    List<UserAsyncExecutor> userAsyncExecutors;

    private String token;

    @BeforeAll
    void init() {
        Namespace ns1 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns1")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-1"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user1")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        Namespace ns2 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns2")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-2"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user2")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicIntegrationTest.BearerAccessRefreshToken> response = ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .POST("/login", credentials), TopicIntegrationTest.BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(ns1));

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(ns2));

        ResourceQuota rqNs2 = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .name("rqNs2")
                .namespace("ns2")
                .build())
            .spec(Map.of(
                ResourceQuota.ResourceQuotaSpecKey.USER_PRODUCER_BYTE_RATE.getKey(), "204800.0",
                ResourceQuota.ResourceQuotaSpecKey.USER_CONSUMER_BYTE_RATE.getKey(), "409600.0"))
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns2/resource-quotas")
                .bearerAuth(token)
                .body(rqNs2));

        Namespace ns3 = Namespace.builder()
            .metadata(Metadata.builder()
                .name("ns3")
                .cluster("test-cluster")
                .labels(Map.of("support-group", "LDAP-GROUP-3"))
                .build())
            .spec(Namespace.NamespaceSpec.builder()
                .kafkaUser("user3")
                .connectClusters(List.of("test-connect"))
                .topicValidator(TopicValidator.makeDefaultOneBroker())
                .build())
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces")
                .bearerAuth(token)
                .body(ns3));

        userAsyncExecutors.forEach(UserAsyncExecutor::run);
    }

    @Test
    void shouldCheckDefaultQuotas() throws ExecutionException, InterruptedException {
        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
            .describeClientQuotas(ClientQuotaFilter.containsOnly(
                List.of(ClientQuotaFilterComponent.ofEntity("user", "user1"))))
            .entities()
            .get();

        assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        assertTrue(quotas.containsKey("producer_byte_rate"));
        assertEquals(102400.0, quotas.get("producer_byte_rate"));
        assertTrue(quotas.containsKey("consumer_byte_rate"));
        assertEquals(102400.0, quotas.get("consumer_byte_rate"));
    }

    @Test
    void shouldCheckCustomQuotas() throws ExecutionException, InterruptedException {
        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
            .describeClientQuotas(ClientQuotaFilter
                .containsOnly(List.of(ClientQuotaFilterComponent.ofEntity("user", "user2"))))
            .entities()
            .get();

        assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        assertTrue(quotas.containsKey("producer_byte_rate"));
        assertEquals(204800.0, quotas.get("producer_byte_rate"));
        assertTrue(quotas.containsKey("consumer_byte_rate"));
        assertEquals(409600.0, quotas.get("consumer_byte_rate"));
    }

    @Test
    void shouldCheckUpdateQuotas() throws ExecutionException, InterruptedException {
        // Update the namespace user quotas
        ResourceQuota rq3 = ResourceQuota.builder()
            .metadata(Metadata.builder()
                .name("rqNs3")
                .namespace("ns3")
                .build())
            .spec(Map.of(
                ResourceQuota.ResourceQuotaSpecKey.USER_PRODUCER_BYTE_RATE.getKey(), "204800.0",
                ResourceQuota.ResourceQuotaSpecKey.USER_CONSUMER_BYTE_RATE.getKey(), "409600.0"))
            .build();

        ns4KafkaClient
            .toBlocking()
            .exchange(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns3/resource-quotas")
                .bearerAuth(token)
                .body(rq3));

        // Force user sync to force the quota update
        userAsyncExecutors.forEach(UserAsyncExecutor::run);

        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
            .describeClientQuotas(ClientQuotaFilter.containsOnly(
                List.of(ClientQuotaFilterComponent.ofEntity("user", "user3"))))
            .entities()
            .get();

        assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        assertTrue(quotas.containsKey("producer_byte_rate"));
        assertEquals(204800.0, quotas.get("producer_byte_rate"));
        assertTrue(quotas.containsKey("consumer_byte_rate"));
        assertEquals(409600.0, quotas.get("consumer_byte_rate"));
    }

    @Test
    void shouldCreateAndUpdateUser() throws ExecutionException, InterruptedException {
        KafkaUserResetPassword response = ns4KafkaClient
            .toBlocking()
            .retrieve(HttpRequest
                .create(HttpMethod.POST, "/api/namespaces/ns1/users/user1/reset-password")
                .bearerAuth(token), KafkaUserResetPassword.class);

        Map<String, UserScramCredentialsDescription> mapUser = getAdminClient()
            .describeUserScramCredentials(List.of("user1")).all().get();

        assertNotNull(response.getSpec().getNewPassword());
        assertTrue(mapUser.containsKey("user1"));
        assertEquals(ScramMechanism.SCRAM_SHA_512, mapUser.get("user1").credentialInfos().getFirst().mechanism());
        assertEquals(4096, mapUser.get("user1").credentialInfos().getFirst().iterations());
    }

    @Test
    void shouldUpdateUserFailWhenItDoesNotBelongToNamespace() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> ns4KafkaClient
                .toBlocking()
                .retrieve(HttpRequest
                    .create(HttpMethod.POST, "/api/namespaces/ns1/users/user2/reset-password")
                    .bearerAuth(token), KafkaUserResetPassword.class));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Invalid value \"user2\" for field \"user\": user does not belong to namespace.",
            exception.getResponse().getBody(Status.class).get().getDetails().getCauses().getFirst());
    }
}
