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

import static com.michelin.ns4kafka.util.enumation.Kind.KAFKA_USER_API_KEY;

import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyRequest;
import com.michelin.ns4kafka.service.client.confluent.entities.ApiKeyResponse;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingListResponse;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingRequest;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingResponse;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.retry.annotation.Retryable;
import io.netty.channel.ConnectTimeoutException;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Confluent Cloud client. */
@Slf4j
@Singleton
public class ConfluentCloudClient {
    private static final String API_KEY_CREATION = "API key creation";
    private final HttpClient httpClient;
    private final List<ManagedClusterProperties> managedClusterProperties;

    /**
     * Constructor.
     *
     * @param httpClient The HTTP client
     * @param managedClusterProperties The managed cluster properties
     */
    public ConfluentCloudClient(
            @Client(id = "confluent-cloud") HttpClient httpClient,
            List<ManagedClusterProperties> managedClusterProperties) {
        this.httpClient = httpClient;
        this.managedClusterProperties = managedClusterProperties;
    }

    /**
     * List the Role Bindings from crn pattern.
     *
     * @param kafkaCluster The Kafka cluster
     * @return The Role Bindings list
     */
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    public Flux<RoleBindingResponse> listRoleBindings(String kafkaCluster, String crnPattern) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);

        HttpRequest<?> request = HttpRequest.GET(URI.create(
                        StringUtils.prependUri(config.getUrl(), "/iam/v2/role-bindings?crn_pattern=" + crnPattern)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, RoleBindingListResponse.class))
                .flatMapMany(roleBindingListResponse -> Flux.fromIterable(roleBindingListResponse.data()));
    }

    /**
     * Create the Confluent Role Binding.
     *
     * @param kafkaCluster The Kafka cluster
     * @param roleBinding The Role Binding to create
     * @return The created Role Binding
     */
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    public Mono<RoleBindingResponse> createRoleBinding(String kafkaCluster, RoleBinding roleBinding) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);
        RoleBindingRequest body = new RoleBindingRequest(roleBinding, config);

        HttpRequest<?> request = HttpRequest.POST(
                        URI.create(StringUtils.prependUri(config.getUrl(), "/iam/v2/role-bindings")), body)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, RoleBindingResponse.class));
    }

    /**
     * Delete the Confluent Role Binding.
     *
     * @param kafkaCluster The Kafka cluster
     * @param roleBindingId The Role Binding id to delete
     * @return The deleted Role Binding
     */
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    public Mono<RoleBindingResponse> deleteRoleBinding(String kafkaCluster, String roleBindingId) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);

        HttpRequest<?> request = HttpRequest.DELETE(
                        URI.create(StringUtils.prependUri(config.getUrl(), "/iam/v2/role-bindings/" + roleBindingId)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, RoleBindingResponse.class));
    }

    /**
     * Delete the Confluent Role Binding.
     *
     * @param kafkaCluster The Kafka cluster
     * @param roleBinding The role binding to delete
     * @return The deleted Role Binding
     */
    public Mono<RoleBindingResponse> deleteRoleBinding(String kafkaCluster, RoleBinding roleBinding) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);
        RoleBindingRequest rbRequest = new RoleBindingRequest(roleBinding, config);

        return listRoleBindings(kafkaCluster, rbRequest.crnPattern())
                .filter(response -> response.crnPattern().equals(rbRequest.crnPattern()))
                .next()
                .flatMap(response -> deleteRoleBinding(kafkaCluster, response.id()));
    }

    /**
     * Get the Confluent Cloud API config of the given Kafka cluster.
     *
     * @param kafkaCluster The Kafka cluster
     * @return The Confluent Cloud API configuration
     */
    private ManagedClusterProperties.ConfluentCloudProperties getConfluentCloud(String kafkaCluster) {
        Optional<ManagedClusterProperties> config = managedClusterProperties.stream()
                .filter(properties -> properties.getName().equals(kafkaCluster))
                .findFirst();

        if (config.isEmpty()) {
            throw new ResourceValidationException(
                    null, null, List.of("Kafka Cluster [" + kafkaCluster + "] not found"));
        }

        if (config.get().getConfluentCloud() == null) {
            throw new ResourceValidationException(
                    null, null, List.of("Kafka Cluster [" + kafkaCluster + "] has no Confluent Cloud Client"));
        }

        return config.get().getConfluentCloud();
    }

    /** Create once: retrying a timed-out POST could create another key whose secret is unavailable. */
    public ApiKeyResponse createApiKey(String kafkaCluster, String owner) {
        if (owner == null || !owner.matches("sa-[a-zA-Z0-9]+")) {
            throw new ResourceValidationException(
                    KAFKA_USER_API_KEY,
                    owner,
                    "API key creation requires a service account ID (sa-...). Identity pools are unsupported.");
        }
        var cluster = managedClusterProperties.stream()
                .filter(properties -> properties.getName().equals(kafkaCluster))
                .findFirst();
        if (cluster.isEmpty() || !cluster.get().isConfluentCloud()) {
            throw new ResourceValidationException(
                    KAFKA_USER_API_KEY, owner, "API key creation is supported only for Confluent Cloud clusters.");
        }
        var config = getConfluentCloud(kafkaCluster);
        if (StringUtils.isEmpty(config.getUrl())
                || StringUtils.isEmpty(config.getBasicAuthUsername())
                || StringUtils.isEmpty(config.getBasicAuthPassword())
                || StringUtils.isEmpty(config.getClusterId())
                || StringUtils.isEmpty(config.getEnvironmentId())) {
            throw new ResourceValidationException(
                    KAFKA_USER_API_KEY,
                    owner,
                    "API key creation requires Confluent URL, management credentials, cluster ID, and environment ID.");
        }
        ApiKeyRequest body = new ApiKeyRequest(new ApiKeyRequest.Spec(
                owner,
                "Created by ns4kafka for cluster " + kafkaCluster,
                new ApiKeyRequest.Owner(owner),
                new ApiKeyRequest.Resource(config.getClusterId(), config.getEnvironmentId())));
        String stage = "service account lookup";
        try {
            HttpRequest<?> lookup = HttpRequest.GET(
                            URI.create(StringUtils.prependUri(config.getUrl(), "/iam/v2/service-accounts/" + owner)))
                    .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
            ApiKeyRequest.Owner account = Mono.from(httpClient.retrieve(lookup, ApiKeyRequest.Owner.class))
                    .block();
            if (account == null || !owner.equals(account.id())) {
                throw new IllegalStateException("Invalid service account response");
            }
            stage = API_KEY_CREATION;
            HttpRequest<?> request = HttpRequest.POST(
                            URI.create(StringUtils.prependUri(config.getUrl(), "/iam/v2/api-keys")), body)
                    .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());
            ApiKeyResponse key = Mono.from(httpClient.retrieve(request, ApiKeyResponse.class))
                    .block();
            if (key == null
                    || StringUtils.isEmpty(key.id())
                    || key.spec() == null
                    || StringUtils.isEmpty(key.spec().secret())
                    || key.spec().owner() == null
                    || !owner.equals(key.spec().owner().id())
                    || key.spec().resource() == null
                    || !config.getClusterId().equals(key.spec().resource().id())) {
                throw new IllegalStateException("Incomplete API key creation response");
            }
            return key;
        } catch (HttpClientResponseException exception) {
            int status = exception.code();
            String guidance =
                    switch (status) {
                        case 401 -> "Check the configured Cloud management API key and secret.";
                        case 403 ->
                            "Check the service account ID (not its display name) and the management account's permissions.";
                        case 400, 404, 422 -> "Check the service account ID, Kafka cluster ID, and environment ID.";
                        case 402, 409, 429 -> "Check Confluent API key quotas and request limits.";
                        default -> "Check Confluent availability before retrying.";
                    };
            throw new HttpStatusException(
                    HttpStatus.BAD_GATEWAY,
                    "Confluent " + stage + " returned HTTP " + status + ". " + guidance
                            + " Existing keys are unchanged."
                            + (API_KEY_CREATION.equals(stage) && status >= 500
                                    ? " Creation outcome is unknown; check Confluent before retrying."
                                    : ""));
        } catch (RuntimeException exception) {
            throw apiKeyFailure(stage, exception);
        }
    }

    private HttpStatusException apiKeyFailure(String stage, RuntimeException exception) {
        boolean timeout = false;
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            timeout |= cause instanceof ReadTimeoutException
                    || cause instanceof ConnectTimeoutException
                    || cause instanceof java.util.concurrent.TimeoutException
                    || cause instanceof java.net.SocketTimeoutException;
        }
        // Never expose upstream response bodies or exception causes containing credentials.
        return new HttpStatusException(
                timeout ? HttpStatus.GATEWAY_TIMEOUT : HttpStatus.BAD_GATEWAY,
                "Confluent " + stage + " failed (" + exception.getClass().getSimpleName()
                        + "). Existing keys are unchanged. "
                        + (API_KEY_CREATION.equals(stage)
                                ? "A new key may have been created; its secret cannot be retrieved. Check Confluent before retrying."
                                : "No key creation was attempted."));
    }
}
