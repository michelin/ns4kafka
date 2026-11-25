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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.client.confluent.entities.ConfluentCloudErrorResponse;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicResponse;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.json.JsonMapper;
import io.micronaut.json.tree.JsonNode;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ConfluentCloudClientTest {
    @Mock
    HttpClient httpClient;

    @Test
    void shouldPatchOnlySnapshotRetentionAndClusterScope() {
        ManagedClusterProperties properties =
                new ManagedClusterProperties("cloud", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        ManagedClusterProperties.ConfluentCloudProperties cloud =
                new ManagedClusterProperties.ConfluentCloudProperties();
        cloud.setEnvironmentId("env-123");
        cloud.setClusterId("lkc-123");
        cloud.setBasicAuthUsername("test");
        cloud.setBasicAuthPassword("test");
        properties.setConfluentCloud(cloud);
        ConfluentCloudClient client = new ConfluentCloudClient(httpClient, List.of(properties));
        JsonMapper mapper = JsonMapper.createDefault();

        when(httpClient.retrieve(
                        any(HttpRequest.class),
                        eq(Argument.of(TableflowTopicResponse.class)),
                        eq(Argument.of(ConfluentCloudErrorResponse.class))))
                .thenAnswer(invocation -> {
                    HttpRequest<?> request = invocation.getArgument(0);
                    assertEquals(HttpMethod.PATCH, request.getMethod());
                    assertEquals(
                            "https://api.confluent.cloud/tableflow/v1/tableflow-topics/ns1.orders",
                            request.getUri().toString());
                    JsonNode actual = mapper.writeValueToTree(request.getBody().orElseThrow());
                    JsonNode expected = mapper.readValue("""
                            {"spec":{
                              "environment":{"id":"env-123"},
                              "kafka_cluster":{"id":"lkc-123"},
                              "config":{"retention_ms":"172800000"}
                            }}
                            """, JsonNode.class);
                    assertEquals(expected, actual);
                    return Mono.just(mapper.readValue("""
                            {"spec":{"config":{"retention_ms":"172800000"}}}
                            """, TableflowTopicResponse.class));
                });

        TableflowTopicResponse response =
                client.updateTableflowTopic("cloud", "ns1.orders", 172_800_000L).block();
        assertEquals("172800000", response.spec().config().retentionMs());
    }
}
