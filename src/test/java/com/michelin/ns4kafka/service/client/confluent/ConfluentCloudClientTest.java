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
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import com.sun.net.httpserver.HttpServer;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
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
        ManagedClusterProperties.ConfluentCloudProperties config =
                new ManagedClusterProperties.ConfluentCloudProperties();
        config.setClusterId("lkc-test");
        config.setEnvironmentId("env-test");
        config.setBasicAuthUsername("global-admin-key");
        config.setBasicAuthPassword("global-admin-secret");
        properties.setConfluentCloud(config);
        HttpClient localClient = mock(HttpClient.class);
        when(localClient.retrieve(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(Class.class)))
                .thenAnswer(invocation -> {
                    HttpRequest<?> request = invocation.getArgument(0);
                    Class<?> responseType = invocation.getArgument(1);
                    assertEquals("api.confluent.cloud", request.getUri().getHost());
                    return httpClient.retrieve(
                            request.toMutableRequest()
                                    .uri(URI.create(url
                                            + request.getUri().getRawPath()
                                            + (request.getUri().getRawQuery() == null
                                                    ? ""
                                                    : "?" + request.getUri().getRawQuery()))),
                            responseType);
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

    @ParameterizedTest
    @ValueSource(strings = {"ICEBERG", "DELTA"})
    void shouldCreateTableflowWithSharedCredentialsAndValidatedSettings(String format) throws Exception {
        replies.add(
                new Reply(
                        202,
                        "{\"id\":\"tt-test\",\"status\":{\"phase\":\"PROVISIONING\",\"error_message\":\"\",\"write_mode\":\"APPEND\"}}"));
        TableflowTopic resource = tableflowTopic("orders.created");
        resource.getSpec().setTableFormats(List.of(format));

        TableflowTopicResponse result =
                client.createTableflowTopic("cloud", resource).block();

        assertEquals("tt-test", result.id());
        assertEquals("PROVISIONING", result.status().phase());
        assertEquals("", result.status().errorMessage());
        assertEquals(1, requests.size());
        Request request = requests.element();
        assertEquals("POST", request.method());
        assertEquals("/tableflow/v1/tableflow-topics", request.uri().getPath());
        assertEquals(authorization(), request.authorization());
        TableflowTopicRequest body = JsonMapper.createDefault().readValue(request.body(), TableflowTopicRequest.class);
        assertEquals("orders.created", body.spec().displayName());
        assertEquals("Managed", body.spec().storage().kind());
        assertEquals(List.of(format), body.spec().tableFormats());
        assertEquals("env-test", body.spec().environment().id());
        assertEquals("lkc-test", body.spec().kafkaCluster().id());
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 409, 422, 429, 500})
    void shouldPropagateTableflowCreationErrorsWithoutRetry(int status) {
        replies.add(new Reply(status, "{}"));
        TableflowTopic resource = tableflowTopic("orders");

        HttpClientResponseException error = assertThrows(
                HttpClientResponseException.class,
                () -> client.createTableflowTopic("cloud", resource).block());

        assertEquals(status, error.code());
        assertEquals(1, requests.size());
    }

    @Test
    void shouldPropagateTableflowTimeoutWithoutRetry() {
        HttpClient http = mock(HttpClient.class);
        when(http.retrieve(
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.eq(TableflowTopicResponse.class)))
                .thenReturn(Mono.error(ReadTimeoutException.TIMEOUT_EXCEPTION));
        ConfluentCloudClient timedOut = new ConfluentCloudClient(http, List.of(properties));
        TableflowTopic resource = tableflowTopic("orders");

        assertThrows(
                ReadTimeoutException.class,
                () -> timedOut.createTableflowTopic("cloud", resource).block());

        verify(http)
                .retrieve(
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.eq(TableflowTopicResponse.class));
    }

    private record Request(String method, URI uri, String authorization, String body) {}

    private static String authorization() {
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString("global-admin-key:global-admin-secret".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldListAllPagesWithTheConfiguredScopeAndSharedCredentials() {
        replies.add(new Reply(200, """
                {"data":[{"spec":{"display_name":"orders","table_formats":["ICEBERG"],"storage":{"kind":"Managed","table_path":"ignored"}},"status":{"phase":"RUNNING"}}],
                "metadata":{"next":"https://api.confluent.cloud/tableflow/v1/tableflow-topics?page_token=next%2B%2F%3D&other=value"}}
                """));
        replies.add(new Reply(200, """
                {"data":[{"spec":{"display_name":"invoices","table_formats":["ICEBERG"],"storage":{"kind":"Managed"}},"status":{"phase":"PENDING"}}],"metadata":{}}
                """));

        List<TableflowTopicResponse> result =
                client.listTableflowTopics("cloud").collectList().block();

        assertEquals(
                List.of("orders", "invoices"),
                result.stream().map(topic -> topic.spec().displayName()).toList());
        assertEquals(List.of("ICEBERG"), result.getFirst().spec().tableFormats());
        assertEquals("Managed", result.getFirst().spec().storage().kind());
        assertEquals("RUNNING", result.getFirst().status().phase());
        assertEquals(2, requests.size());
        List<Request> captured = List.copyOf(requests);
        assertEquals(
                "environment=env-test&spec.kafka_cluster=lkc-test&page_size=100",
                captured.getFirst().uri().getRawQuery());
        assertEquals(
                "environment=env-test&spec.kafka_cluster=lkc-test&page_size=100&page_token=next%2B%2F%3D",
                captured.getLast().uri().getRawQuery());
        captured.forEach(request -> {
            assertEquals("GET", request.method());
            assertEquals("/tableflow/v1/tableflow-topics", request.uri().getPath());
            assertEquals(authorization(), request.authorization());
        });
    }

    @Test
    void shouldReturnAnEmptyListWhenNoTableflowTopicsExist() {
        replies.add(new Reply(200, "{\"data\":[]}"));
        assertTrue(client.listTableflowTopics("cloud").collectList().block().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 404, 429, 500})
    void shouldPropagateListingFailuresInsteadOfReturningAnEmptyList(int status) {
        replies.add(new Reply(status, "{}"));

        HttpClientResponseException error = assertThrows(
                HttpClientResponseException.class,
                () -> client.listTableflowTopics("cloud").collectList().block());

        assertEquals(status, error.code());
        assertEquals(1, requests.size());
    }

    @Test
    void shouldFailTheCompleteListingWhenALaterPageFails() {
        replies.add(new Reply(200, """
                {"data":[{"spec":{"display_name":"orders"}}],"metadata":{"next":"https://api.confluent.cloud/tableflow/v1/tableflow-topics?page_token=next"}}
                """));
        replies.add(new Reply(503, "{}"));

        assertThrows(
                HttpClientResponseException.class,
                () -> client.listTableflowTopics("cloud").collectList().block());
        assertEquals(2, requests.size());
    }
}
