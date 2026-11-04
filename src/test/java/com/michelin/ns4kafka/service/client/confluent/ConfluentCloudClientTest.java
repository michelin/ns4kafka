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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyMetadata;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ConfluentCloudClientTest {
    private static final String OWNER = "sa-test";
    private static final String KEY = "KEY123";
    private static final String CLUSTER = "lkc-test";
    private static final String URI = "https://api.confluent.cloud/iam/v2/api-keys/" + KEY;

    @Mock
    HttpClient http;

    private ManagedClusterProperties cluster;
    private ConfluentCloudClient client;

    @BeforeEach
    void setUp() {
        cluster = new ManagedClusterProperties("cloud", ManagedClusterProperties.KafkaProvider.CONFLUENT_CLOUD);
        ManagedClusterProperties.ConfluentCloudProperties config =
                new ManagedClusterProperties.ConfluentCloudProperties();
        config.setBasicAuthUsername("management");
        config.setBasicAuthPassword("password");
        config.setClusterId(CLUSTER);
        cluster.setConfluentCloud(config);
        client = new ConfluentCloudClient(http, List.of(cluster));
    }

    private void lookup(String key, String owner, String resource) {
        ApiKeyMetadata.Spec spec = new ApiKeyMetadata.Spec(
                new ApiKeyMetadata.Reference(owner), resource == null ? null : new ApiKeyMetadata.Reference(resource));
        when(http.retrieve(
                        argThat((HttpRequest<?> req) -> req.getMethod() == HttpMethod.GET
                                && URI.equals(req.getUri().toString())),
                        eq(ApiKeyMetadata.class)))
                .thenReturn(Mono.just(new ApiKeyMetadata(key, spec)));
    }

    @Test
    void shouldDeleteOnlyTheVerifiedKey() {
        lookup(KEY, OWNER, CLUSTER);
        when(http.exchange(
                        argThat((HttpRequest<?> req) -> req.getMethod() == HttpMethod.DELETE
                                && URI.equals(req.getUri().toString())),
                        eq(Void.class)))
                .thenReturn(Mono.just(HttpResponse.noContent()));
        assertEquals(
                Boolean.TRUE, client.deleteApiKey("cloud", OWNER, KEY, false).block());
        verify(http).exchange(any(HttpRequest.class), eq(Void.class));
    }

    @Test
    void shouldOnlyReadDuringDryRun() {
        lookup(KEY, OWNER, CLUSTER);
        assertEquals(
                Boolean.TRUE, client.deleteApiKey("cloud", OWNER, KEY, true).block());
        verify(http, never()).exchange(any(HttpRequest.class), eq(Void.class));
    }

    @ParameterizedTest
    @CsvSource({"OTHER,sa-test,lkc-test", "KEY123,sa-other,lkc-test", "KEY123,sa-test,lkc-dr", "KEY123,sa-test,"})
    void shouldNotDeleteAnUnverifiedKey(String key, String owner, String resource) {
        lookup(key, owner, resource);
        assertEquals(
                Boolean.FALSE, client.deleteApiKey("cloud", OWNER, KEY, false).block());
        verify(http, never()).exchange(any(HttpRequest.class), eq(Void.class));
    }

    @Test
    void shouldNotDeleteAfterLookupFailure() {
        when(http.retrieve(any(HttpRequest.class), eq(ApiKeyMetadata.class)))
                .thenReturn(Mono.error(new HttpClientResponseException("upstream", HttpResponse.notFound())));
        assertEquals(
                Boolean.FALSE, client.deleteApiKey("cloud", OWNER, KEY, false).block());
        verify(http, never()).exchange(any(HttpRequest.class), eq(Void.class));
    }

    @Test
    void shouldPropagateDeletionFailureWithoutRetry() {
        lookup(KEY, OWNER, CLUSTER);
        when(http.exchange(any(HttpRequest.class), eq(Void.class)))
                .thenReturn(Mono.error(
                        new HttpClientResponseException("upstream", HttpResponse.status(HttpStatus.FORBIDDEN))));
        HttpClientResponseException error = assertThrows(
                HttpClientResponseException.class,
                () -> client.deleteApiKey("cloud", OWNER, KEY, false).block());
        assertEquals(HttpStatus.BAD_GATEWAY, error.getStatus());
        verify(http).exchange(any(HttpRequest.class), eq(Void.class));
    }

    @Test
    void shouldRejectUnsupportedTargetsBeforeLookup() {
        assertThrows(ResourceValidationException.class, () -> client.deleteApiKey("cloud", "pool-test", KEY, false));
        assertThrows(ResourceValidationException.class, () -> client.deleteApiKey("cloud", OWNER, "../other", false));
        cluster.getConfluentCloud().setClusterId("GLOBAL");
        assertThrows(ResourceValidationException.class, () -> client.deleteApiKey("cloud", OWNER, KEY, false));
        cluster.getConfluentCloud().setClusterId(CLUSTER);
        cluster.setProvider(ManagedClusterProperties.KafkaProvider.SELF_MANAGED);
        assertThrows(ResourceValidationException.class, () -> client.deleteApiKey("cloud", OWNER, KEY, false));
        verifyNoInteractions(http);
    }
}
