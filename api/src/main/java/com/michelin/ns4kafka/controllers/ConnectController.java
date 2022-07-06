package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.ChangeConnectorState;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.KafkaConnectService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Tag(name = "Connects")
@Controller(value = "/api/namespaces/{namespace}/connects")
@ExecuteOn(TaskExecutors.IO)
public class ConnectController extends NamespacedResourceController {
    /**
     * The connector service
     */
    @Inject
    KafkaConnectService kafkaConnectService;

    /**
     * Get all the connectors by namespace
     * @param namespace The namespace
     * @return A list of connectors
     */
    @Get
    public List<Connector> list(String namespace) {
        return kafkaConnectService.findAllForNamespace(getNamespace(namespace));
    }

    /**
     * Get the last version of a connector by namespace and name
     * @param namespace The namespace
     * @param connector The name
     * @return A connector
     */
    @Get("/{connector}")
    public Optional<Connector> getConnector(String namespace, String connector) {
        return kafkaConnectService.findByName(getNamespace(namespace), connector);
    }

    /**
     * Delete all connectors under the given name
     * @param namespace The current namespace
     * @param connector The current connector name to delete
     * @param dryrun Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{connector}{?dryrun}")
    public HttpResponse<Void> deleteConnector(String namespace, String connector, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate ownership
        if (!kafkaConnectService.isNamespaceOwnerOfConnect(ns, connector)) {
            throw new ResourceValidationException(List.of("Invalid value " + connector +
                    " for name: Namespace not OWNER of this connector"), "Connector", connector);
        }

        Optional<Connector> optionalConnector = kafkaConnectService.findByName(ns, connector);

        if (optionalConnector.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        Connector connectorToDelete = optionalConnector.get();
        sendEventLog(connectorToDelete.getKind(),
                connectorToDelete.getMetadata(),
                ApplyStatus.deleted,
                connectorToDelete.getSpec(),
                null);

        kafkaConnectService.delete(ns, optionalConnector.get());

        return HttpResponse.noContent();
    }

    /**
     * Publish a connector
     * @param namespace The namespace
     * @param connector  The connector to create
     * @param dryrun Does the creation is a dry run
     * @return The created schema
     */
    @Post("{?dryrun}")
    public HttpResponse<Connector> apply(String namespace, @Valid @Body Connector connector, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        // Validate ownership
        if (!kafkaConnectService.isNamespaceOwnerOfConnect(ns, connector.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid value " + connector.getMetadata().getName() +
                    " for name: Namespace not OWNER of this connector"), connector.getKind(), connector.getMetadata().getName());
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
        List<String> validationErrors = kafkaConnectService.validateLocally(ns, connector);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, connector.getKind(), connector.getMetadata().getName());
        }

        // Validate against connect rest API /validate
        validationErrors = kafkaConnectService.validateRemotely(ns, connector);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, connector.getKind(), connector.getMetadata().getName());
        }

        // Augment with server side fields
        connector.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        connector.getMetadata().setCluster(ns.getMetadata().getCluster());
        connector.getMetadata().setNamespace(ns.getMetadata().getName());
        connector.setStatus(Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.UNASSIGNED)
                .build());

        Optional<Connector> existingConnector = kafkaConnectService.findByName(ns, connector.getMetadata().getName());
        if (existingConnector.isPresent() && existingConnector.get().equals(connector)) {
            return formatHttpResponse(existingConnector.get(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingConnector.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
        if (dryrun) {
            return formatHttpResponse(connector, status);
        }

        sendEventLog(connector.getKind(),
                connector.getMetadata(),
                status,
                existingConnector.<Object>map(Connector::getSpec).orElse(null),
                connector.getSpec());

        return formatHttpResponse(kafkaConnectService.createOrUpdate(connector), status);
    }

    /**
     * Change the state of a connector
     * @param namespace The namespace
     * @param connector The connector to update the state
     * @param changeConnectorState The state to set
     * @return
     */
    @Post("/{connector}/change-state")
    public HttpResponse<ChangeConnectorState> changeState(String namespace, String connector, @Body @Valid ChangeConnectorState changeConnectorState) {
        Namespace ns = getNamespace(namespace);

        if (!kafkaConnectService.isNamespaceOwnerOfConnect(ns, connector)) {
            throw new ResourceValidationException(List.of("Invalid value " + connector +
                    " for name: Namespace not OWNER of this connector"), "Connector", connector);
        }

        Optional<Connector> optionalConnector = kafkaConnectService.findByName(ns, connector);

        if (optionalConnector.isEmpty()) {
            return HttpResponse.notFound();
        }

        HttpResponse response;
        try {
            switch (changeConnectorState.getSpec().getAction()) {
                case restart:
                    response = kafkaConnectService.restart(ns, optionalConnector.get());
                    break;
                case pause:
                    response = kafkaConnectService.pause(ns, optionalConnector.get());
                    break;
                case resume:
                    response = kafkaConnectService.resume(ns, optionalConnector.get());
                    break;
                default:
                    throw new IllegalStateException("Unspecified Action "+changeConnectorState.getSpec().getAction());
            }

            changeConnectorState.setStatus(ChangeConnectorState.ChangeConnectorStateStatus.builder()
                    .success(true)
                    .code(response.status())
                    .build());
        } catch (Exception e) {
            changeConnectorState.setStatus(ChangeConnectorState.ChangeConnectorStateStatus.builder()
                    .success(false)
                    .code(HttpStatus.INTERNAL_SERVER_ERROR)
                    .errorMessage(e.getMessage())
                    .build());
        }

        changeConnectorState.setMetadata(optionalConnector.get().getMetadata());
        changeConnectorState.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        return HttpResponse.ok(changeConnectorState);
    }

    /**
     * Import unsynchronized connectors
     * @param namespace The namespace
     * @param dryrun Is dry run mode or not ?
     * @return The list of imported connectors
     */
    @Post("/_/import{?dryrun}")
    public List<Connector> importResources(String namespace, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);
        List<Connector> unsynchronizedConnectors = kafkaConnectService.listUnsynchronizedConnectors(ns);

        unsynchronizedConnectors.forEach(connector -> {
            connector.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            connector.getMetadata().setCluster(ns.getMetadata().getCluster());
            connector.getMetadata().setNamespace(ns.getMetadata().getName());
        });

        if (dryrun) {
            return unsynchronizedConnectors;
        }

        return unsynchronizedConnectors
                .stream()
                .map(connector -> {
                    sendEventLog(connector.getKind(), connector.getMetadata(), ApplyStatus.created, null, connector.getSpec());
                    return kafkaConnectService.createOrUpdate(connector);
                })
                .collect(Collectors.toList());
    }
}
