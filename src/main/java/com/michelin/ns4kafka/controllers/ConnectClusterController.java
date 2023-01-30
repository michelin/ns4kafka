package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.ConnectCluster;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.services.ConnectClusterService;
import com.michelin.ns4kafka.services.ConnectorService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import com.nimbusds.jose.JOSEException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;

import javax.validation.Valid;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Tag(name = "Connect Clusters")
@Controller(value = "/api/namespaces/{namespace}/connect-clusters")
@ExecuteOn(TaskExecutors.IO)
public class ConnectClusterController extends NamespacedResourceController {
    @Inject
    ConnectClusterService connectClusterService;

    @Inject
    ConnectorService connectorService;

    /**
     * Get all the Connect workers by namespace
     *
     * @param namespace The namespace
     * @return A list of connectors
     */
    @Get
    public List<ConnectCluster> list(String namespace) {
        return connectClusterService.findAllByNamespaceOwner(getNamespace(namespace));
    }

    /**
     * Get the last version of a connector by namespace and name
     *
     * @param namespace      The namespace
     * @param connectCluster The name
     * @return A Connect worker
     */
    @Get("/{connectCluster}")
    public Optional<ConnectCluster> getConnectCluster(String namespace, String connectCluster) {
        return connectClusterService.findByNamespaceAndNameOwner(getNamespace(namespace), connectCluster);
    }

    /**
     * Publish a Connect worker
     *
     * @param namespace      The namespace
     * @param connectCluster The connect worker
     * @param dryrun         Does the creation is a dry run
     * @return The created role binding
     */
    @Post("/{?dryrun}")
    HttpResponse<ConnectCluster> apply(String namespace, @Body @Valid ConnectCluster connectCluster, @QueryValue(defaultValue = "false") boolean dryrun) throws IOException, JOSEException {
        Namespace ns = getNamespace(namespace);

        List<String> validationErrors = new ArrayList<>();
        if (!connectClusterService.isNamespaceOwnerOfConnectCluster(ns, connectCluster.getMetadata().getName())) {
            validationErrors.add(String.format("Namespace not owner of this Connect cluster %s.", connectCluster.getMetadata().getName()));
        }

        validationErrors.addAll(connectClusterService.validateConnectClusterCreation(connectCluster));

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, connectCluster.getKind(), connectCluster.getMetadata().getName());
        }

        connectCluster.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        connectCluster.getMetadata().setCluster(ns.getMetadata().getCluster());
        connectCluster.getMetadata().setNamespace(ns.getMetadata().getName());

        Optional<ConnectCluster> existingConnectCluster = connectClusterService.findByNamespaceAndNameOwner(ns, connectCluster.getMetadata().getName());
        if (existingConnectCluster.isPresent() && existingConnectCluster.get().equals(connectCluster)) {
            return formatHttpResponse(existingConnectCluster.get(), ApplyStatus.unchanged);
        }

        ApplyStatus status = existingConnectCluster.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
        if (dryrun) {
            return formatHttpResponse(connectCluster, status);
        }

        sendEventLog(connectCluster.getKind(), connectCluster.getMetadata(), status, existingConnectCluster.<Object>map(ConnectCluster::getSpec).orElse(null),
                connectCluster.getSpec());

        return formatHttpResponse(connectClusterService.create(connectCluster), status);
    }

    /**
     * Delete Connect cluster by the given name
     *
     * @param namespace      The current namespace
     * @param connectCluster The current connect cluster name to delete
     * @param dryrun         Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{connectCluster}{?dryrun}")
    public HttpResponse<Void> delete(String namespace, String connectCluster, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        List<String> validationErrors = new ArrayList<>();
        if (!connectClusterService.isNamespaceOwnerOfConnectCluster(ns, connectCluster)) {
            validationErrors.add(String.format("Namespace not owner of this Connect cluster %s.", connectCluster));
        }

        List<Connector> connectors = connectorService.findAllByConnectCluster(ns, connectCluster);
        if (!connectors.isEmpty()) {
            validationErrors.add(String.format("The Connect cluster %s has %s deployed connector(s): %s. Please remove the associated connector(s) before deleting it.", connectCluster, connectors.size(),
                    connectors.stream().map(connector -> connector.getMetadata().getName()).collect(Collectors.joining(", "))));
        }

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, "ConnectCluster", connectCluster);
        }

        Optional<ConnectCluster> optionalConnectCluster = connectClusterService.findByNamespaceAndNameOwner(ns, connectCluster);
        if (optionalConnectCluster.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        ConnectCluster connectClusterToDelete = optionalConnectCluster.get();
        sendEventLog(connectClusterToDelete.getKind(), connectClusterToDelete.getMetadata(), ApplyStatus.deleted, connectClusterToDelete.getSpec(), null);

        connectClusterService.delete(connectClusterToDelete);
        return HttpResponse.noContent();
    }

    /**
     * Gets the list of available Kafka Connect vaults for a namespace
     *
     * @return The list of the available Kafka Connect vaults.
     */
    @Get("/_/vaults")
    public List<ConnectCluster> listVaults(final String namespace) {
        final Namespace ns = getNamespace(namespace);
        return connectClusterService.findAllByNamespaceWrite(ns)
                .stream()
                .filter(connectCluster -> StringUtils.hasText(connectCluster.getSpec().getAes256Key()))
                .toList();
    }

    /**
     * Encrypt a password for a specific Kafka Connect cluster.
     *
     * @param namespace      The namespace.
     * @param connectCluster The name of the Kafka Connect cluster.
     * @param password       The password to encrypt.
     * @return The encrypted password.
     */
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Post("/{connectCluster}/vaults")
    public String vaultPasswordJson(final String namespace, final String connectCluster, final String password) {
        final Namespace ns = getNamespace(namespace);

        final var validationErrors = new ArrayList<String>();
        if (!connectClusterService.isNamespaceAllowedForConnectCluster(ns, connectCluster)) {
            validationErrors.add(String.format("Namespace is not allowed to use this Connect cluster %s.", connectCluster));
        }

        validationErrors.addAll(connectClusterService.validateConnectClusterVault(ns, connectCluster));

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors, "ConnectCluster", connectCluster);
        }

        return connectClusterService.vaultPassword(ns, connectCluster, password);
    }
}
