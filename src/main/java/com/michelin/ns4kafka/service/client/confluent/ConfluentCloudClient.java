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

import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBinding;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingListResponse;
import com.michelin.ns4kafka.service.client.confluent.entities.RoleBindingResponse;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.micronaut.retry.annotation.Retryable;
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
     * List the Role Bindings.
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
     * @param roleBinding The role binding to create
     */
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    public void createRoleBinding(String kafkaCluster, RoleBinding roleBinding) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);

        HttpRequest<?> request = HttpRequest.POST(
                        URI.create(StringUtils.prependUri(config.getUrl(), "/iam/v2/role-bindings")), roleBinding)
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * Delete the Confluent Role Binding.
     *
     * @param kafkaCluster The Kafka cluster
     * @param roleBindingId The role binding id to delete
     * @return The deleted Confluent Role Binding
     */
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    public Mono<HttpResponse<Void>> deleteRoleBinding(String kafkaCluster, String roleBindingId) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);

        HttpRequest<?> request = HttpRequest.DELETE(
                        URI.create(StringUtils.prependUri(config.getUrl(), "/iam/v2/role-bindings/" + roleBindingId)))
                .basicAuth(config.getBasicAuthUsername(), config.getBasicAuthPassword());

        return Mono.from(httpClient.exchange(request, Void.class));
    }

    /**
     * Delete the Confluent Role Binding.
     *
     * @param kafkaCluster The Kafka cluster
     * @param roleBinding The role binding to delete
     */
    public void deleteRoleBinding(String kafkaCluster, RoleBinding roleBinding) {
        ManagedClusterProperties.ConfluentCloudProperties config = getConfluentCloud(kafkaCluster);
        String organizationId = config.getOrganizationId();
        String environmentId = config.getEnvironmentId();
        String clusterId = config.getClusterId();

        listRoleBindings(kafkaCluster, roleBinding.getCrnPattern(organizationId, environmentId, clusterId))
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
}
