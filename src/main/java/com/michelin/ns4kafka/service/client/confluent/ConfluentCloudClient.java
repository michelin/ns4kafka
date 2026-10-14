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

import static com.michelin.ns4kafka.util.enumation.Kind.TABLEFLOW_TOPIC;

import com.michelin.ns4kafka.model.TableflowTopic;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingListResponse;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingRequest;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingResponse;
import com.michelin.ns4kafka.service.client.confluent.entities.TableflowTopicRequest;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.retry.annotation.Retryable;
import io.netty.channel.ConnectTimeoutException;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Confluent Cloud client. */
@Slf4j
@Singleton
public class ConfluentCloudClient {
    private static final String CONFLUENT_CLOUD_API_URL = "https://api.confluent.cloud";
    private static final String PAGE_TOKEN_PARAM = "page_token=";
    private static final int PAGE_SIZE = 1000;

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
     * Submit Tableflow creation once; acceptance does not mean the table is ready to query.
     *
     * @param kafkaCluster The Kafka cluster
     * @param tableflowTopic The validated Tableflow topic
     * @return The Confluent Tableflow creation response
     */
    public JsonNode createTableflowTopic(String kafkaCluster, TableflowTopic tableflowTopic) {
        String topic = tableflowTopic.getMetadata().getName();
        var cluster = managedClusterProperties.stream()
                .filter(properties -> properties.getName().equals(kafkaCluster))
                .findFirst();
        if (cluster.isEmpty() || !cluster.get().isConfluentCloud()) {
            throw new ResourceValidationException(
                    TABLEFLOW_TOPIC, topic, "Tableflow is supported only for Confluent Cloud clusters.");
        }
        var config = getConfluentCloud(kafkaCluster);
        var credentials = config.getTableflow();
        if (StringUtils.isEmpty(config.getClusterId())
                || StringUtils.isEmpty(config.getEnvironmentId())
                || credentials == null
                || StringUtils.isEmpty(credentials.getBasicAuthUsername())
                || StringUtils.isEmpty(credentials.getBasicAuthPassword())) {
            throw new ResourceValidationException(
                    TABLEFLOW_TOPIC,
                    topic,
                    "Tableflow creation requires cluster ID, environment ID, and Tableflow credentials.");
        }
        var body = new TableflowTopicRequest(new TableflowTopicRequest.Spec(
                topic,
                new TableflowTopicRequest.Storage(
                        tableflowTopic.getSpec().getStorage().getKind()),
                tableflowTopic.getSpec().getTableFormats(),
                new TableflowTopicRequest.Reference(config.getEnvironmentId()),
                new TableflowTopicRequest.Reference(config.getClusterId())));
        HttpRequest<?> request = HttpRequest.POST(
                        URI.create(StringUtils.prependUri(CONFLUENT_CLOUD_API_URL, "/tableflow/v1/tableflow-topics")),
                        body)
                .basicAuth(credentials.getBasicAuthUsername(), credentials.getBasicAuthPassword());
        try {
            var response =
                    Mono.from(httpClient.retrieve(request, JsonNode.class)).block();
            if (response == null || !response.isObject()) {
                throw new IllegalStateException("Invalid Tableflow creation response");
            }
            return response;
        } catch (HttpClientResponseException exception) {
            int status = exception.code();
            throw new HttpStatusException(
                    status == 409 ? HttpStatus.CONFLICT : HttpStatus.BAD_GATEWAY,
                    "Confluent Tableflow creation returned HTTP " + status + ". "
                            + (status == 409
                                    ? "Tableflow already exists for this topic."
                                    : "Check the Tableflow credentials, permissions, topic schemas, and Confluent availability."));
        } catch (RuntimeException exception) {
            boolean timeout = false;
            for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                timeout |= cause instanceof ReadTimeoutException
                        || cause instanceof ConnectTimeoutException
                        || cause instanceof java.util.concurrent.TimeoutException
                        || cause instanceof java.net.SocketTimeoutException;
            }
            // Do not expose upstream response bodies or exception causes containing credentials.
            throw new HttpStatusException(
                    timeout ? HttpStatus.GATEWAY_TIMEOUT : HttpStatus.BAD_GATEWAY,
                    "Confluent Tableflow creation " + (timeout ? "timed out" : "failed")
                            + ". Check Confluent before retrying; creation may have been accepted.");
        }
    }

    /**
     * List all the role bindings matching a crn pattern, following the pagination.
     *
     * @param kafkaCluster The Kafka cluster
     * @param crnPattern The crn pattern, partially matched
     * @return The role bindings list
     */
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    public Flux<RoleBindingResponse> listRoleBindings(String kafkaCluster, String crnPattern) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);

        return listRoleBindingsPage(config, crnPattern, null)
                .expand(response -> {
                    String pageToken = response.metadata() != null
                            ? extractPageToken(response.metadata().next())
                            : null;
                    return pageToken != null ? listRoleBindingsPage(config, crnPattern, pageToken) : Mono.empty();
                })
                .flatMapIterable(response -> response.data() != null ? response.data() : List.of());
    }

    /**
     * Create the Confluent role binding.
     *
     * @param kafkaCluster The Kafka cluster
     * @param roleBinding The role binding to create
     * @return The created role binding
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
                        URI.create(StringUtils.prependUri(CONFLUENT_CLOUD_API_URL, "/iam/v2/role-bindings")), body)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, RoleBindingResponse.class));
    }

    /**
     * Delete the Confluent role binding.
     *
     * @param kafkaCluster The Kafka cluster
     * @param roleBindingId The role binding id to delete
     * @return The deleted role binding
     */
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    public Mono<RoleBindingResponse> deleteRoleBinding(String kafkaCluster, String roleBindingId) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);

        HttpRequest<?> request = HttpRequest.DELETE(URI.create(
                        StringUtils.prependUri(CONFLUENT_CLOUD_API_URL, "/iam/v2/role-bindings/" + roleBindingId)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, RoleBindingResponse.class));
    }

    /**
     * List one page of role bindings matching a crn pattern.
     *
     * @param config The Confluent Cloud properties
     * @param crnPattern The crn pattern
     * @param pageToken The page token, or null for the first page
     * @return The role bindings page
     */
    private Mono<RoleBindingListResponse> listRoleBindingsPage(
            ManagedClusterProperties.ConfluentCloudProperties config, String crnPattern, @Nullable String pageToken) {
        HttpRequest<?> request = HttpRequest.GET(URI.create(StringUtils.prependUri(
                        CONFLUENT_CLOUD_API_URL,
                        "/iam/v2/role-bindings?crn_pattern=" + crnPattern
                                + (pageToken != null ? "&page_token=" + pageToken : "&page_size=" + PAGE_SIZE))))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.retrieve(request, RoleBindingListResponse.class));
    }

    /**
     * Extract the page token from the link to the next page.
     *
     * @param next The link to the next page
     * @return The raw page token, or null if there is no next page
     */
    private static @Nullable String extractPageToken(@Nullable String next) {
        if (next == null || URI.create(next).getRawQuery() == null) {
            return null;
        }

        return Arrays.stream(URI.create(next).getRawQuery().split("&"))
                .filter(param -> param.startsWith(PAGE_TOKEN_PARAM))
                .map(param -> param.substring(PAGE_TOKEN_PARAM.length()))
                .filter(pageToken -> !pageToken.isEmpty())
                .findFirst()
                .orElse(null);
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
}
