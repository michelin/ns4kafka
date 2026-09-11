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
package com.michelin.ns4kafka.service.client.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import com.sun.net.httpserver.HttpServer;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.json.JsonMapper;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;

class ConfluentCloudClientTest {
    HttpServer server;
    HttpClient httpClient;
    ConfluentCloudClient client;
    ManagedClusterProperties properties;
    final ConcurrentLinkedQueue<Reply> replies = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Request> requests = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(new Request(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI(),
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            Reply reply = replies.poll();
            if (reply == null) {
                reply = new Reply(500, "{}");
            }
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] body = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(reply.status(), reply.status() == 204 ? -1 : body.length);
            if (reply.status() != 204) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
        String url = "http://127.0.0.1:" + server.getAddress().getPort();
        httpClient = HttpClient.create(URI.create(url).toURL());
        properties = new ManagedClusterProperties("cloud", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        var config = new ManagedClusterProperties.ConfluentCloudProperties();
        config.setUrl(url);
        config.setClusterId("lkc-test");
        config.setEnvironmentId("env-test");
        config.setBasicAuthUsername("management");
        config.setBasicAuthPassword("test-secret");
        properties.setConfluentCloud(config);
        client = new ConfluentCloudClient(httpClient, List.of(properties));
    }

    @AfterEach
    void tearDown() {
        if (httpClient != null) {
            httpClient.close();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    String key(String id, String secret) {
        return """
                {"id":"%s","spec":{"secret":"%s","owner":{"id":"sa-test"},
                 "resource":{"id":"lkc-test","environment":"env-test"}}}
                """.formatted(id, secret);
    }

    @Test
    void shouldCreateAdditionalKeysWithoutListingOrDeleting() throws Exception {
        replies.add(new Reply(200, "{\"id\":\"sa-test\"}"));
        replies.add(new Reply(202, key("KEY1", "SECRET1")));
        replies.add(new Reply(200, "{\"id\":\"sa-test\"}"));
        replies.add(new Reply(202, key("KEY2", "SECRET2")));
        assertEquals("KEY1", client.createApiKey("cloud", "sa-test").id());
        assertEquals("KEY2", client.createApiKey("cloud", "sa-test").id());
        assertEquals(
                List.of("GET", "POST", "GET", "POST"),
                requests.stream().map(Request::method).toList());
        for (Request request :
                requests.stream().filter(r -> r.method().equals("POST")).toList()) {
            assertEquals("/iam/v2/api-keys", request.uri().getPath());
            assertEquals(
                    "Basic "
                            + Base64.getEncoder()
                                    .encodeToString("management:test-secret".getBytes(StandardCharsets.UTF_8)),
                    request.authorization());
            var body = JsonMapper.createDefault()
                    .readValue(
                            request.body(),
                            com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyRequest.class);
            assertEquals("sa-test", body.spec().owner().id());
            assertEquals("sa-test", body.spec().displayName());
            assertEquals("lkc-test", body.spec().resource().id());
            assertEquals("env-test", body.spec().resource().environment());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"pool-test", "u-test", "display-name", "sa-"})
    void shouldRejectUnsupportedPrincipals(String owner) {
        assertThrows(ResourceValidationException.class, () -> client.createApiKey("cloud", owner));
        assertTrue(requests.isEmpty());
    }

    @Test
    void shouldRejectMissingConfigurationAndOtherProviders() {
        properties.getConfluentCloud().setBasicAuthPassword(null);
        assertThrows(ResourceValidationException.class, () -> client.createApiKey("cloud", "sa-test"));
        properties.setProvider(ManagedClusterProperties.KafkaProvider.SELF_MANAGED);
        assertThrows(ResourceValidationException.class, () -> client.createApiKey("cloud", "sa-test"));
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 429, 500})
    void shouldSanitizeErrorsWithoutRetry(int status) {
        replies.add(new Reply(200, "{\"id\":\"sa-test\"}"));
        replies.add(new Reply(status, "{\"message\":\"DO-NOT-EXPOSE\"}"));
        var error = assertThrows(HttpStatusException.class, () -> client.createApiKey("cloud", "sa-test"));
        assertEquals(HttpStatus.BAD_GATEWAY, error.getStatus());
        assertTrue(!error.getMessage().contains("DO-NOT-EXPOSE"));
        assertEquals(null, error.getCause());
        assertTrue(error.getMessage().contains("API key creation returned HTTP " + status));
        assertEquals(2, requests.size());
    }

    @Test
    void shouldRejectInvalidAccountBeforeCreatingAKey() {
        replies.add(new Reply(403, "{\"message\":\"DO-NOT-EXPOSE\"}"));
        var error = assertThrows(HttpStatusException.class, () -> client.createApiKey("cloud", "sa-test"));
        assertTrue(error.getMessage().contains("service account lookup returned HTTP 403"));
        assertTrue(error.getMessage().contains("not its display name"));
        assertEquals(List.of("GET"), requests.stream().map(Request::method).toList());
    }

    @Test
    void shouldRejectIncompleteCreationResponse() {
        replies.add(new Reply(200, "{\"id\":\"sa-test\"}"));
        replies.add(new Reply(202, key("KEY", "")));
        assertEquals(
                HttpStatus.BAD_GATEWAY,
                assertThrows(HttpStatusException.class, () -> client.createApiKey("cloud", "sa-test"))
                        .getStatus());
        assertEquals(2, requests.size());
    }

    @Test
    void shouldReportTimeoutWithoutRetry() {
        var http = org.mockito.Mockito.mock(HttpClient.class);
        org.mockito.Mockito.when(http.retrieve(
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.eq(
                                com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyResponse.class)))
                .thenReturn(Mono.error(ReadTimeoutException.TIMEOUT_EXCEPTION));
        org.mockito.Mockito.when(http.retrieve(
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.eq(
                                com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyRequest.Owner.class)))
                .thenReturn(Mono.just(
                        new com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyRequest.Owner("sa-test")));
        var timedOut = new ConfluentCloudClient(http, List.of(properties));
        assertEquals(
                HttpStatus.GATEWAY_TIMEOUT,
                assertThrows(HttpStatusException.class, () -> timedOut.createApiKey("cloud", "sa-test"))
                        .getStatus());
        org.mockito.Mockito.verify(http)
                .retrieve(
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.eq(
                                com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyResponse.class));
    }

    private record Reply(int status, String body) {}

    private record Request(String method, URI uri, String authorization, String body) {}
}
