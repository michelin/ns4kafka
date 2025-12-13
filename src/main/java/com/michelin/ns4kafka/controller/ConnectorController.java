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
package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidOwner;
import static com.michelin.ns4kafka.util.enumation.Kind.CONNECTOR;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.connector.ChangeConnectorState;
import com.michelin.ns4kafka.model.connector.Connector;
import com.michelin.ns4kafka.service.ConnectorService;
import com.michelin.ns4kafka.service.ResourceQuotaService;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Controller to manage connectors. */
@Tag(name = "Connectors", description = "Manage the connectors.")
@Controller(value = "/api/namespaces/{namespace}/connectors")
@ExecuteOn(TaskExecutors.IO)
public class ConnectorController extends NamespacedResourceController {
    @Inject
    private ConnectorService connectorService;

    @Inject
    private ResourceQuotaService resourceQuotaService;

    /**
     * List connectors by namespace, filtered by name parameter.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @return A list of connectors
     */
    @Get
    public List<Connector> list(String namespace, @QueryValue(defaultValue = "*") String name) {
        return connectorService.findByWildcardName(getNamespace(namespace), name);
    }

    /**
     * Get a connector by namespace and name.
     *
     * @param namespace The namespace
     * @param connector The name
     * @return A connector
     * @deprecated use {@link #list(String, String)} instead.
     */
    @Get("/{connector}")
    @Deprecated(since = "1.12.0")
    public Optional<Connector> get(String namespace, String connector) {
        return connectorService.findByName(getNamespace(namespace), connector);
    }

    /**
     * Create a connector.
     *
     * @param namespace The namespace
     * @param connector The connector to create
     * @param dryrun Is dry run mode or not?
     * @return The created connector
     */
    @Post("{?dryrun}")
    public Mono<HttpResponse<Connector>> apply(
            String namespace, @Valid @Body Connector connector, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        if (!connectorService.isNamespaceOwnerOfConnect(
                ns, connector.getMetadata().getName())) {
            return Mono.error(new ResourceValidationException(
                    connector, invalidOwner(connector.getMetadata().getName())));
        }

        // Set / Override name in spec.config.name, required for several Kafka Connect API calls
        // This is a response to projects setting a value A in metadata.name, and a value B in spec.config.name
        // I have considered alternatives :
        // - Make name in spec.config forbidden, and set it only when necessary (API calls)
        // - Mask it in the resulting list connectors so that the synchronization process doesn't see changes
        // I prefer to go this way for 2 reasons:
        // - It is backward compatible with teams that already define name in spec.config.name
        // - It doesn't impact the code as much (single line vs 10+ lines)
        connector.getSpec().getConfig().put("name", connector.getMetadata().getName());

        // Validate locally
        return connectorService.validateLocally(ns, connector).flatMap(validationErrors -> {
            if (!validationErrors.isEmpty()) {
                return Mono.error(new ResourceValidationException(connector, validationErrors));
            }

            // Validate against connect rest API /validate
            return connectorService.validateRemotely(ns, connector).flatMap(remoteValidationErrors -> {
                if (!remoteValidationErrors.isEmpty()) {
                    return Mono.error(new ResourceValidationException(connector, remoteValidationErrors));
                }

                // Augment with server side fields
                connector.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                connector.getMetadata().setCluster(ns.getMetadata().getCluster());
                connector.getMetadata().setNamespace(ns.getMetadata().getName());
                connector.setStatus(Connector.ConnectorStatus.builder()
                        .state(Connector.TaskState.UNASSIGNED)
                        .build());

                Optional<Connector> existingConnector =
                        connectorService.findByName(ns, connector.getMetadata().getName());
                if (existingConnector.isPresent() && existingConnector.get().equals(connector)) {
                    return Mono.just(formatHttpResponse(existingConnector.get(), ApplyStatus.UNCHANGED));
                }

                ApplyStatus status = existingConnector.isPresent() ? ApplyStatus.CHANGED : ApplyStatus.CREATED;

                // Only check quota on connector creation
                if (status.equals(ApplyStatus.CREATED)) {
                    List<String> quotaErrors = resourceQuotaService.validateConnectorQuota(ns);
                    if (!quotaErrors.isEmpty()) {
                        return Mono.error(new ResourceValidationException(connector, quotaErrors));
                    }
                }

                if (dryrun) {
                    return Mono.just(formatHttpResponse(connector, status));
                }

                // Set a toDeploy flag
                // Without this trick, if only sensitive fields were updated, the executor wouldn't apply the difference
                connector.getStatus().setToDeploy(true);

                sendEventLog(
                        connector,
                        status,
                        existingConnector.<Object>map(Connector::getSpec).orElse(null),
                        connector.getSpec(),
                        EMPTY_STRING);

                return Mono.just(formatHttpResponse(connectorService.createOrUpdate(connector), status));
            });
        });
    }

    /**
     * Delete a connector.
     *
     * @param namespace The current namespace
     * @param connector The current connector name to delete
     * @param dryrun Is dry run mode or not?
     * @return A HTTP response
     * @deprecated use {@link #bulkDelete(String, String, boolean)} instead.
     */
    @Delete("/{connector}{?dryrun}")
    @Deprecated(since = "1.13.0")
    @Status(HttpStatus.NO_CONTENT)
    public Mono<HttpResponse<Void>> delete(
            String namespace, String connector, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate ownership
        if (!connectorService.isNamespaceOwnerOfConnect(ns, connector)) {
            return Mono.error(new ResourceValidationException(CONNECTOR, connector, invalidOwner(connector)));
        }

        Optional<Connector> optionalConnector = connectorService.findByName(ns, connector);
        if (optionalConnector.isEmpty()) {
            return Mono.just(HttpResponse.notFound());
        }

        if (dryrun) {
            return Mono.just(HttpResponse.noContent());
        }

        Connector connectorToDelete = optionalConnector.get();

        sendEventLog(connectorToDelete, ApplyStatus.DELETED, connectorToDelete.getSpec(), null, EMPTY_STRING);

        return connectorService.delete(ns, optionalConnector.get()).map(_ -> HttpResponse.noContent());
    }

    /**
     * Delete connectors.
     *
     * @param namespace The current namespace
     * @param name The name parameter
     * @param dryrun Run in dry mode or not?
     * @return A HTTP response
     */
    @Delete
    @Status(HttpStatus.OK)
    public Mono<HttpResponse<List<Connector>>> bulkDelete(
            String namespace,
            @QueryValue(defaultValue = "*") String name,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        List<Connector> connectors = connectorService.findByWildcardName(ns, name);

        // Validate ownership
        List<String> validationErrors = connectors.stream()
                .filter(connector -> !connectorService.isNamespaceOwnerOfConnect(
                        ns, connector.getMetadata().getName()))
                .map(connector -> invalidOwner(connector.getMetadata().getName()))
                .toList();

        if (!validationErrors.isEmpty()) {
            return Mono.error(new ResourceValidationException(CONNECTOR, name, validationErrors));
        }

        if (connectors.isEmpty()) {
            return Mono.just(HttpResponse.notFound());
        }

        if (dryrun) {
            return Mono.just(HttpResponse.ok(connectors));
        }

        return Flux.fromIterable(connectors)
                .flatMap(connector -> {
                    sendEventLog(connector, ApplyStatus.DELETED, connector.getSpec(), null, EMPTY_STRING);
                    return connectorService.delete(ns, connector);
                })
                .then(Mono.just(HttpResponse.ok(connectors)));
    }

    /**
     * Change the state of a connector.
     *
     * @param namespace The namespace
     * @param connector The connector to update the state
     * @param state The state to set
     * @return The change connector state response
     */
    @Post("/{connector}/change-state")
    public Mono<MutableHttpResponse<ChangeConnectorState>> changeState(
            String namespace, String connector, @Body @Valid ChangeConnectorState state) {
        Namespace ns = getNamespace(namespace);

        if (!connectorService.isNamespaceOwnerOfConnect(ns, connector)) {
            return Mono.error(new ResourceValidationException(CONNECTOR, connector, invalidOwner(connector)));
        }

        Optional<Connector> optionalConnector = connectorService.findByName(ns, connector);

        if (optionalConnector.isEmpty()) {
            return Mono.just(HttpResponse.notFound());
        }

        Mono<HttpResponse<Void>> response;
        switch (state.getSpec().getAction()) {
            case RESTART -> response = connectorService.restart(ns, optionalConnector.get());
            case PAUSE -> response = connectorService.pause(ns, optionalConnector.get());
            case RESUME -> response = connectorService.resume(ns, optionalConnector.get());
            default -> {
                return Mono.error(new IllegalStateException(
                        "Unspecified action " + state.getSpec().getAction()));
            }
        }

        return response.doOnSuccess(success -> {
                    state.setStatus(ChangeConnectorState.ChangeConnectorStateStatus.builder()
                            .success(true)
                            .code(success.status())
                            .build());
                    state.setMetadata(optionalConnector.get().getMetadata());
                    state.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                })
                .doOnError(error -> {
                    state.setStatus(ChangeConnectorState.ChangeConnectorStateStatus.builder()
                            .success(false)
                            .code(HttpStatus.INTERNAL_SERVER_ERROR)
                            .errorMessage(error.getMessage())
                            .build());
                    state.setMetadata(optionalConnector.get().getMetadata());
                    state.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                })
                .map(_ -> HttpResponse.ok(state))
                .onErrorReturn(HttpResponse.ok(state));
    }

    /**
     * Import unsynchronized connectors.
     *
     * @param namespace The namespace
     * @param name The name parameter
     * @param dryrun Is dry run mode or not?
     * @return The list of imported connectors
     */
    @Post("/_/import")
    public Flux<Connector> importResources(
            String namespace,
            @QueryValue(defaultValue = "*") String name,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);
        return connectorService
                .listUnsynchronizedConnectorsByWildcardName(ns, name)
                .map(unsynchronizedConnector -> {
                    unsynchronizedConnector.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                    unsynchronizedConnector
                            .getMetadata()
                            .setCluster(ns.getMetadata().getCluster());
                    unsynchronizedConnector
                            .getMetadata()
                            .setNamespace(ns.getMetadata().getName());

                    if (dryrun) {
                        return unsynchronizedConnector;
                    }

                    sendEventLog(
                            unsynchronizedConnector,
                            ApplyStatus.CREATED,
                            null,
                            unsynchronizedConnector.getSpec(),
                            EMPTY_STRING);

                    return connectorService.createOrUpdate(unsynchronizedConnector);
                });
    }
}
