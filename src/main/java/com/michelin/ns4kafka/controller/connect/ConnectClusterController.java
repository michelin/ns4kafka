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
package com.michelin.ns4kafka.controller.connect;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidConnectClusterDeleteOperation;
import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.util.enumation.Kind.CONNECT_CLUSTER;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.model.connect.cluster.VaultResponse;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.service.ConnectClusterService;
import com.michelin.ns4kafka.service.ConnectorService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import reactor.core.publisher.Mono;

/** Controller to manage Kafka Connect clusters. */
@Tag(name = "Connect Clusters", description = "Manage the Kafka Connect clusters.")
@Controller(value = "/api/namespaces/{namespace}/connect-clusters")
@ExecuteOn(TaskExecutors.IO)
public class ConnectClusterController extends NamespacedResourceController {
    @Inject
    private ConnectClusterService connectClusterService;

    @Inject
    private ConnectorService connectorService;

    /**
     * List Kafka Connect clusters by namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @return A list of Kafka Connect clusters
     */
    @Get
    public List<ConnectCluster> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        return connectClusterService.findByWildcardNameWithOwnerPermission(getNamespace(namespace), name);
    }

    /**
     * Get a Kafka Connect clusters by namespace and name.
     *
     * @param namespace The namespace
     * @param connectCluster The name
     * @return A Kafka Connect cluster
     * @deprecated use list(String, String name) instead.
     */
    @Get("/{connectCluster}")
    @Deprecated(since = "1.12.0")
    public Optional<ConnectCluster> get(String namespace, String connectCluster) {
        return connectClusterService.findByNameWithOwnerPermission(getNamespace(namespace), connectCluster);
    }

    /**
     * Create a Kafka Connect cluster.
     *
     * @param namespace The namespace
     * @param connectCluster The connect worker
     * @param dryrun Is dry run mode or not?
     * @return The created Kafka Connect cluster
     */
    @Post("/{?dryrun}")
    public Mono<HttpResponse<ConnectCluster>> apply(
            String namespace,
            @Body @Valid ConnectCluster connectCluster,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        List<String> validationErrors = new ArrayList<>();
        if (!connectClusterService.isNamespaceOwnerOfConnectCluster(
                ns, connectCluster.getMetadata().getName())) {
            validationErrors.add(invalidOwner(connectCluster.getMetadata().getName()));
        }

        return connectClusterService
                .validateConnectClusterCreation(connectCluster)
                .flatMap(errors -> {
                    validationErrors.addAll(errors);
                    if (!validationErrors.isEmpty()) {
                        return Mono.error(new ResourceValidationException(connectCluster, validationErrors));
                    }

                    connectCluster.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                    connectCluster.getMetadata().setCluster(ns.getMetadata().getCluster());
                    connectCluster.getMetadata().setNamespace(ns.getMetadata().getName());

                    Optional<ConnectCluster> existingConnectCluster =
                            connectClusterService.findByNameWithOwnerPermission(
                                    ns, connectCluster.getMetadata().getName());
                    if (existingConnectCluster.isPresent()
                            && existingConnectCluster.get().equals(connectCluster)) {
                        return Mono.just(formatHttpResponse(existingConnectCluster.get(), ApplyStatus.UNCHANGED));
                    }

                    ApplyStatus status = existingConnectCluster.isPresent() ? ApplyStatus.CHANGED : ApplyStatus.CREATED;
                    if (dryrun) {
                        return Mono.just(formatHttpResponse(connectCluster, status));
                    }

                    sendEventLog(
                            connectCluster,
                            status,
                            existingConnectCluster
                                    .<Object>map(ConnectCluster::getSpec)
                                    .orElse(null),
                            connectCluster.getSpec(),
                            EMPTY_STRING);

                    return Mono.just(formatHttpResponse(connectClusterService.create(connectCluster), status));
                });
    }

    /**
     * Delete a Kafka Connect cluster.
     *
     * @param namespace The current namespace
     * @param connectCluster The current connect cluster name to delete
     * @param dryrun Run in dry mode or not
     * @return A HTTP response
     * @deprecated use {@link #bulkDelete(String, String, boolean)} instead.
     */
    @Delete("/{connectCluster}{?dryrun}")
    @Deprecated(since = "1.13.0")
    public HttpResponse<Void> delete(
            String namespace, String connectCluster, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        List<String> validationErrors = new ArrayList<>();
        if (!connectClusterService.isNamespaceOwnerOfConnectCluster(ns, connectCluster)) {
            validationErrors.add(invalidOwner(connectCluster));
        }

        List<Connector> connectors = connectorService.findAllByConnectCluster(ns, connectCluster);
        if (!connectors.isEmpty()) {
            validationErrors.add(invalidConnectClusterDeleteOperation(connectCluster, connectors));
        }

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(CONNECT_CLUSTER, connectCluster, validationErrors);
        }

        Optional<ConnectCluster> optionalConnectCluster =
                connectClusterService.findByNameWithOwnerPermission(ns, connectCluster);

        if (optionalConnectCluster.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        sendEventLog(
                optionalConnectCluster.get(),
                ApplyStatus.DELETED,
                optionalConnectCluster.get().getSpec(),
                null,
                EMPTY_STRING);

        connectClusterService.delete(optionalConnectCluster.get());

        return HttpResponse.noContent();
    }

    /**
     * Delete Kafka Connect clusters.
     *
     * @param namespace The current namespace
     * @param name The name parameter
     * @param dryrun Run in dry mode or not
     * @return A HTTP response
     */
    @Delete
    public HttpResponse<List<ConnectCluster>> bulkDelete(
            String namespace,
            @QueryValue(defaultValue = "*") String name,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        List<ConnectCluster> connectClusters = connectClusterService.findByWildcardNameWithOwnerPermission(ns, name);

        List<String> validationErrors = new ArrayList<>();
        connectClusters.forEach(cc -> {
            List<Connector> connectors = connectorService.findAllByConnectCluster(
                    ns, cc.getMetadata().getName());
            if (!connectors.isEmpty()) {
                validationErrors.add(
                        invalidConnectClusterDeleteOperation(cc.getMetadata().getName(), connectors));
            }
        });

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(CONNECT_CLUSTER, name, validationErrors);
        }

        if (connectClusters.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.ok(connectClusters);
        }

        connectClusters.forEach(cc -> {
            sendEventLog(cc, ApplyStatus.DELETED, cc.getSpec(), null, EMPTY_STRING);

            connectClusterService.delete(cc);
        });

        return HttpResponse.ok(connectClusters);
    }

    /**
     * List vault Kafka Connect clusters by namespace.
     *
     * @return A list of the available vault Kafka Connect clusters
     */
    @Get("/_/vaults")
    public List<ConnectCluster> listVaults(final String namespace) {
        return connectClusterService.findAllForNamespaceWithWritePermission(getNamespace(namespace)).stream()
                .filter(connectCluster ->
                        StringUtils.hasText(connectCluster.getSpec().getAes256Key())
                                && StringUtils.hasText(connectCluster.getSpec().getAes256Salt()))
                .toList();
    }

    /**
     * Encrypt a list of passwords.
     *
     * @param namespace The namespace.
     * @param connectCluster The name of the Kafka Connect cluster.
     * @param passwords The passwords to encrypt.
     * @return The encrypted password.
     */
    @Post("/{connectCluster}/vaults")
    public List<VaultResponse> vaultPassword(
            final String namespace, final String connectCluster, @Body final List<String> passwords) {
        final Namespace ns = getNamespace(namespace);

        List<String> validationErrors = connectClusterService.validateConnectClusterVault(ns, connectCluster);

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(CONNECT_CLUSTER, connectCluster, validationErrors);
        }

        return connectClusterService.vaultPassword(ns, connectCluster, passwords);
    }
}
