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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicRequest;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import com.sun.net.httpserver.HttpServer;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.json.JsonMapper;
import io.micronaut.json.tree.JsonNode;
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
        config.setClusterId("lkc-test");
        config.setEnvironmentId("env-test");
        config.setBasicAuthUsername("management");
        config.setBasicAuthPassword("test-secret");
        properties.setConfluentCloud(config);
        var localClient = mock(HttpClient.class);
        when(localClient.retrieve(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(JsonNode.class)))
                .thenAnswer(invocation -> {
                    HttpRequest<?> request = invocation.getArgument(0);
                    assertEquals("api.confluent.cloud", request.getUri().getHost());
                    return httpClient.retrieve(
                            request.toMutableRequest()
                                    .uri(URI.create(url
                                            + request.getUri().getRawPath()
                                            + (request.getUri().getRawQuery() == null
                                                    ? ""
                                                    : "?" + request.getUri().getRawQuery()))),
                            JsonNode.class);
                });
        client = new ConfluentCloudClient(localClient, List.of(properties));
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

    private record Reply(int status, String body) {}

    private static TableflowTopic tableflowTopic(String name) {
        return TableflowTopic.builder()
                .metadata(Resource.Metadata.builder().name(name).build())
                .spec(TableflowTopic.TableflowTopicSpec.builder()
                        .tableFormats(List.of("ICEBERG"))
                        .storage(
                                TableflowTopic.Storage.builder().kind("Managed").build())
                        .build())
                .build();
    }

    void configureTableflow() {
        var credentials = new ManagedClusterProperties.ConfluentCloudProperties.TableflowProperties();
        credentials.setBasicAuthUsername("tableflow-key");
        credentials.setBasicAuthPassword("tableflow-secret");
        properties.getConfluentCloud().setTableflow(credentials);
    }

    @ParameterizedTest
    @ValueSource(strings = {"ICEBERG", "DELTA"})
    void shouldCreateTableflowWithDedicatedCredentialsAndValidatedSettings(String format) throws Exception {
        configureTableflow();
        replies.add(new Reply(202, "{\"id\":\"tt-test\",\"status\":{\"phase\":\"PROVISIONING\"}}"));

        TableflowTopic resource = tableflowTopic("orders.created");
        resource.getSpec().setTableFormats(List.of(format));
        JsonNode result = client.createTableflowTopic("cloud", resource);

        assertEquals("tt-test", result.get("id").getStringValue());
        assertEquals(1, requests.size());
        Request request = requests.element();
        assertEquals("POST", request.method());
        assertEquals("/tableflow/v1/tableflow-topics", request.uri().getPath());
        assertEquals(
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString("tableflow-key:tableflow-secret".getBytes(StandardCharsets.UTF_8)),
                request.authorization());
        var body = JsonMapper.createDefault().readValue(request.body(), TableflowTopicRequest.class);
        assertEquals("orders.created", body.spec().displayName());
        assertEquals("Managed", body.spec().storage().kind());
        assertEquals(List.of(format), body.spec().tableFormats());
        assertEquals("env-test", body.spec().environment().id());
        assertEquals("lkc-test", body.spec().kafkaCluster().id());
    }

    @Test
    void shouldRejectTableflowWithoutDedicatedCredentialsOrCloudProvider() {
        TableflowTopic resource = tableflowTopic("orders");
        assertThrows(ResourceValidationException.class, () -> client.createTableflowTopic("cloud", resource));
        configureTableflow();
        properties.setProvider(ManagedClusterProperties.KafkaProvider.SELF_MANAGED);
        assertThrows(ResourceValidationException.class, () -> client.createTableflowTopic("cloud", resource));
        assertThrows(ResourceValidationException.class, () -> client.createTableflowTopic("unknown", resource));
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"cluster", "environment", "key", "secret"})
    void shouldRejectIncompleteTableflowConfigurationBeforeSending(String field) {
        configureTableflow();
        var config = properties.getConfluentCloud();
        switch (field) {
            case "cluster" -> config.setClusterId(null);
            case "environment" -> config.setEnvironmentId(null);
            case "key" -> config.getTableflow().setBasicAuthUsername(null);
            case "secret" -> config.getTableflow().setBasicAuthPassword(null);
            default -> throw new IllegalArgumentException(field);
        }
        TableflowTopic resource = tableflowTopic("orders");
        assertThrows(ResourceValidationException.class, () -> client.createTableflowTopic("cloud", resource));
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 409, 422, 429, 500})
    void shouldSanitizeTableflowErrorsWithoutRetry(int status) {
        configureTableflow();
        replies.add(new Reply(status, "{\"message\":\"DO-NOT-EXPOSE\"}"));
        TableflowTopic resource = tableflowTopic("orders");
        var error = assertThrows(HttpStatusException.class, () -> client.createTableflowTopic("cloud", resource));
        assertEquals(status == 409 ? HttpStatus.CONFLICT : HttpStatus.BAD_GATEWAY, error.getStatus());
        assertTrue(error.getMessage().contains("HTTP " + status));
        assertTrue(!error.getMessage().contains("DO-NOT-EXPOSE"));
        assertEquals(1, requests.size());
    }

    @Test
    void shouldReportTableflowTimeoutWithoutRetry() {
        configureTableflow();
        var http = mock(HttpClient.class);
        when(http.retrieve(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(JsonNode.class)))
                .thenReturn(Mono.error(ReadTimeoutException.TIMEOUT_EXCEPTION));
        var timedOut = new ConfluentCloudClient(http, List.of(properties));
        TableflowTopic resource = tableflowTopic("orders");
        var error = assertThrows(HttpStatusException.class, () -> timedOut.createTableflowTopic("cloud", resource));
        assertEquals(HttpStatus.GATEWAY_TIMEOUT, error.getStatus());
        assertTrue(error.getMessage().contains("creation may have been accepted"));
        verify(http).retrieve(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(JsonNode.class));
    }

    private record Request(String method, URI uri, String authorization, String body) {}

    @Test
    void shouldReadTableflowWithTheConfiguredScopeAndDedicatedCredentials() {
        configureTableflow();
        replies.add(new Reply(200, "{\"status\":{\"phase\":\"RUNNING\"}}"));

        var result = client.getTableflowTopic("cloud", "orders.created");

        assertEquals("RUNNING", result.orElseThrow().get("status").get("phase").getStringValue());
        Request request = requests.element();
        assertEquals("GET", request.method());
        assertEquals(
                "/tableflow/v1/tableflow-topics/orders.created", request.uri().getPath());
        assertEquals(
                "environment=env-test&spec.kafka_cluster=lkc-test",
                request.uri().getQuery());
        assertEquals(
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString("tableflow-key:tableflow-secret".getBytes(StandardCharsets.UTF_8)),
                request.authorization());
    }

    @Test
    void shouldReturnEmptyOnlyWhenTableflowDoesNotExist() {
        configureTableflow();
        replies.add(new Reply(404, "{}"));
        assertTrue(client.getTableflowTopic("cloud", "orders").isEmpty());
    }

    @Test
    void shouldValidateLocalConfigurationWithoutMakingAnHttpRequest() {
        assertThrows(ResourceValidationException.class, () -> client.validateTableflowConfiguration("cloud", "orders"));
        configureTableflow();
        client.validateTableflowConfiguration("cloud", "orders");
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 429, 500})
    void shouldNotTreatLookupFailuresAsMissingResources(int status) {
        configureTableflow();
        replies.add(new Reply(status, "{\"message\":\"DO-NOT-EXPOSE\"}"));

        var error = assertThrows(HttpStatusException.class, () -> client.getTableflowTopic("cloud", "orders"));

        assertEquals(HttpStatus.BAD_GATEWAY, error.getStatus());
        assertTrue(error.getMessage().contains("HTTP " + status));
        assertTrue(!error.getMessage().contains("DO-NOT-EXPOSE"));
        assertEquals(1, requests.size());
    }
}
