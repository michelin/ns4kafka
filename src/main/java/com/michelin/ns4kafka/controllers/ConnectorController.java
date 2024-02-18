package com.michelin.ns4kafka.controllers;

import static com.michelin.ns4kafka.models.Kind.CONNECTOR;
import static com.michelin.ns4kafka.utils.exceptions.ResourceValidationMessage.INVALID_FIELD;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidOwner;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.connector.ChangeConnectorState;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.services.ConnectorService;
import com.michelin.ns4kafka.services.ResourceQuotaService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
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

/**
 * Controller to manage connectors.
 */
@Tag(name = "Connectors", description = "Manage the connectors.")
@Controller(value = "/api/namespaces/{namespace}/connectors")
@ExecuteOn(TaskExecutors.IO)
public class ConnectorController extends NamespacedResourceController {
    private static final String NAMESPACE_NOT_OWNER = "Namespace not owner of this connector %s.";

    @Inject
    ConnectorService connectorService;

    @Inject
    ResourceQuotaService resourceQuotaService;

    /**
     * List connectors by namespace.
     *
     * @param namespace The namespace
     * @return A list of connectors
     */
    @Get
    public List<Connector> list(String namespace) {
        return connectorService.findAllForNamespace(getNamespace(namespace));
    }

    /**
     * Get a connector by namespace and name.
     *
     * @param namespace The namespace
     * @param connector The name
     * @return A connector
     */
    @Get("/{connector}")
    public Optional<Connector> getConnector(String namespace, String connector) {
        return connectorService.findByName(getNamespace(namespace), connector);
    }

    /**
     * Create a connector.
     *
     * @param namespace The namespace
     * @param connector The connector to create
     * @param dryrun    Does the creation is a dry run
     * @return The created connector
     */
    @Post("{?dryrun}")
    public Mono<HttpResponse<Connector>> apply(String namespace, @Valid @Body Connector connector,
                                               @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        if (!connectorService.isNamespaceOwnerOfConnect(ns, connector.getMetadata().getName())) {
            throw new ResourceValidationException(CONNECTOR, connector.getMetadata().getName(),
                invalidOwner(connector.getMetadata().getName()));
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
        return connectorService.validateLocally(ns, connector)
            .flatMap(validationErrors -> {
                if (!validationErrors.isEmpty()) {
                    return Mono.error(new ResourceValidationException(CONNECTOR, connector.getMetadata().getName(),
                        validationErrors));
                }

                // Validate against connect rest API /validate
                return connectorService.validateRemotely(ns, connector)
                    .flatMap(remoteValidationErrors -> {
                        if (!remoteValidationErrors.isEmpty()) {
                            return Mono.error(
                                new ResourceValidationException(CONNECTOR, connector.getMetadata().getName(),
                                    remoteValidationErrors));
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
                            return Mono.just(formatHttpResponse(existingConnector.get(), ApplyStatus.unchanged));
                        }

                        ApplyStatus status = existingConnector.isPresent() ? ApplyStatus.changed : ApplyStatus.created;

                        // Only check quota on connector creation
                        if (status.equals(ApplyStatus.created)) {
                            List<String> quotaErrors = resourceQuotaService.validateConnectorQuota(ns);
                            if (!quotaErrors.isEmpty()) {
                                return Mono.error(new ResourceValidationException(CONNECTOR,
                                    connector.getMetadata().getName(), quotaErrors));
                            }
                        }

                        if (dryrun) {
                            return Mono.just(formatHttpResponse(connector, status));
                        }

                        sendEventLog(connector.getKind(), connector.getMetadata(), status,
                            existingConnector.<Object>map(Connector::getSpec).orElse(null), connector.getSpec());

                        return Mono.just(formatHttpResponse(connectorService.createOrUpdate(connector), status));
                    });
            });
    }

    /**
     * Delete a connector.
     *
     * @param namespace The current namespace
     * @param connector The current connector name to delete
     * @param dryrun    Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{connector}{?dryrun}")
    public Mono<HttpResponse<Void>> deleteConnector(String namespace, String connector,
                                                    @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate ownership
        if (!connectorService.isNamespaceOwnerOfConnect(ns, connector)) {
            throw new ResourceValidationException(CONNECTOR, connector,
                new InvalidOwner(connector));
        }

        Optional<Connector> optionalConnector = connectorService.findByName(ns, connector);
        if (optionalConnector.isEmpty()) {
            return Mono.just(HttpResponse.notFound());
        }

        if (dryrun) {
            return Mono.just(HttpResponse.noContent());
        }

        Connector connectorToDelete = optionalConnector.get();
        sendEventLog(connectorToDelete.getKind(),
            connectorToDelete.getMetadata(),
            ApplyStatus.deleted,
            connectorToDelete.getSpec(),
            null);

        return connectorService
            .delete(ns, optionalConnector.get())
            .map(httpResponse -> HttpResponse.noContent());
    }

    /**
     * Change the state of a connector.
     *
     * @param namespace            The namespace
     * @param connector            The connector to update the state
     * @param changeConnectorState The state to set
     * @return The change connector state response
     */
    @Post("/{connector}/change-state")
    public Mono<MutableHttpResponse<ChangeConnectorState>> changeState(
        String namespace, String connector, @Body @Valid ChangeConnectorState changeConnectorState) {
        Namespace ns = getNamespace(namespace);

        if (!connectorService.isNamespaceOwnerOfConnect(ns, connector)) {
            throw new ResourceValidationException(List.of(String.format(INVALID_FIELD,
                connector, "name", NAMESPACE_NOT_OWNER)),
                CONNECTOR, connector);
        }

        Optional<Connector> optionalConnector = connectorService.findByName(ns, connector);

        if (optionalConnector.isEmpty()) {
            return Mono.just(HttpResponse.notFound());
        }

        Mono<HttpResponse<Void>> response;
        switch (changeConnectorState.getSpec().getAction()) {
            case restart -> response = connectorService.restart(ns, optionalConnector.get());
            case pause -> response = connectorService.pause(ns, optionalConnector.get());
            case resume -> response = connectorService.resume(ns, optionalConnector.get());
            default -> {
                return Mono.error(
                    new IllegalStateException("Unspecified action " + changeConnectorState.getSpec().getAction()));
            }
        }

        return response
            .doOnSuccess(success -> {
                changeConnectorState.setStatus(ChangeConnectorState.ChangeConnectorStateStatus.builder()
                    .success(true)
                    .code(success.status())
                    .build());
                changeConnectorState.setMetadata(optionalConnector.get().getMetadata());
                changeConnectorState.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            })
            .doOnError(error -> {
                changeConnectorState.setStatus(ChangeConnectorState.ChangeConnectorStateStatus.builder()
                    .success(false)
                    .code(HttpStatus.INTERNAL_SERVER_ERROR)
                    .errorMessage(error.getMessage())
                    .build());
                changeConnectorState.setMetadata(optionalConnector.get().getMetadata());
                changeConnectorState.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            })
            .map(httpResponse -> HttpResponse.ok(changeConnectorState))
            .onErrorReturn(HttpResponse.ok(changeConnectorState));
    }

    /**
     * Import unsynchronized connectors.
     *
     * @param namespace The namespace
     * @param dryrun    Is dry run mode or not ?
     * @return The list of imported connectors
     */
    @Post("/_/import{?dryrun}")
    public Flux<Connector> importResources(String namespace, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);
        return connectorService.listUnsynchronizedConnectors(ns)
            .map(unsynchronizedConnector -> {
                unsynchronizedConnector.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                unsynchronizedConnector.getMetadata().setCluster(ns.getMetadata().getCluster());
                unsynchronizedConnector.getMetadata().setNamespace(ns.getMetadata().getName());

                if (dryrun) {
                    return unsynchronizedConnector;
                }

                sendEventLog(unsynchronizedConnector.getKind(), unsynchronizedConnector.getMetadata(),
                    ApplyStatus.created, null, unsynchronizedConnector.getSpec());

                return connectorService.createOrUpdate(unsynchronizedConnector);
            });
    }
}
