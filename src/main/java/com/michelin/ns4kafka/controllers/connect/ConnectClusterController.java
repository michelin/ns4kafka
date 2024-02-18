package com.michelin.ns4kafka.controllers.connect;

import static com.michelin.ns4kafka.models.Kind.CONNECTOR_CLUSTER;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidConnectClusterDeleteOperation;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidConnectClusterNotAllowed;
import static com.michelin.ns4kafka.utils.exceptions.error.ValidationError.invalidOwner;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.models.connect.cluster.VaultResponse;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.services.ConnectClusterService;
import com.michelin.ns4kafka.services.ConnectorService;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import reactor.core.publisher.Mono;

/**
 * Controller to manage Kafka Connect clusters.
 */
@Tag(name = "Connect Clusters", description = "Manage the Kafka Connect clusters.")
@Controller(value = "/api/namespaces/{namespace}/connect-clusters")
@ExecuteOn(TaskExecutors.IO)
public class ConnectClusterController extends NamespacedResourceController {
    @Inject
    ConnectClusterService connectClusterService;

    @Inject
    ConnectorService connectorService;

    /**
     * List Kafka Connect clusters by namespace.
     *
     * @param namespace The namespace
     * @return A list of Kafka Connect clusters
     */
    @Get
    public List<ConnectCluster> list(String namespace) {
        return connectClusterService.findAllByNamespaceOwner(getNamespace(namespace));
    }

    /**
     * Get a Kafka Connect clusters by namespace and name.
     *
     * @param namespace      The namespace
     * @param connectCluster The name
     * @return A Kafka Connect cluster
     */
    @Get("/{connectCluster}")
    public Optional<ConnectCluster> getConnectCluster(String namespace, String connectCluster) {
        return connectClusterService.findByNamespaceAndNameOwner(getNamespace(namespace), connectCluster);
    }

    /**
     * Create a Kafka Connect cluster.
     *
     * @param namespace      The namespace
     * @param connectCluster The connect worker
     * @param dryrun         Does the creation is a dry run
     * @return The created Kafka Connect cluster
     */
    @Post("/{?dryrun}")
    public Mono<HttpResponse<ConnectCluster>> apply(String namespace, @Body @Valid ConnectCluster connectCluster,
                                                    @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);

        List<String> validationErrors = new ArrayList<>();
        if (!connectClusterService.isNamespaceOwnerOfConnectCluster(ns, connectCluster.getMetadata().getName())) {
            validationErrors.add(invalidOwner(connectCluster.getMetadata().getName()));
        }

        return connectClusterService.validateConnectClusterCreation(connectCluster)
            .flatMap(errors -> {
                validationErrors.addAll(errors);
                if (!validationErrors.isEmpty()) {
                    return Mono.error(new ResourceValidationException(CONNECTOR_CLUSTER,
                        connectCluster.getMetadata().getName(), validationErrors));
                }

                connectCluster.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
                connectCluster.getMetadata().setCluster(ns.getMetadata().getCluster());
                connectCluster.getMetadata().setNamespace(ns.getMetadata().getName());

                Optional<ConnectCluster> existingConnectCluster =
                    connectClusterService.findByNamespaceAndNameOwner(ns, connectCluster.getMetadata().getName());
                if (existingConnectCluster.isPresent() && existingConnectCluster.get().equals(connectCluster)) {
                    return Mono.just(formatHttpResponse(existingConnectCluster.get(), ApplyStatus.unchanged));
                }

                ApplyStatus status = existingConnectCluster.isPresent() ? ApplyStatus.changed : ApplyStatus.created;
                if (dryrun) {
                    return Mono.just(formatHttpResponse(connectCluster, status));
                }

                sendEventLog(connectCluster.getKind(), connectCluster.getMetadata(), status,
                    existingConnectCluster.<Object>map(ConnectCluster::getSpec).orElse(null),
                    connectCluster.getSpec());

                return Mono.just(formatHttpResponse(connectClusterService.create(connectCluster), status));
            });
    }

    /**
     * Delete a Kafka Connect cluster.
     *
     * @param namespace      The current namespace
     * @param connectCluster The current connect cluster name to delete
     * @param dryrun         Run in dry mode or not
     * @return A HTTP response
     */
    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{connectCluster}{?dryrun}")
    public HttpResponse<Void> delete(String namespace, String connectCluster,
                                     @QueryValue(defaultValue = "false") boolean dryrun) {
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
            throw new ResourceValidationException(CONNECTOR_CLUSTER, connectCluster, validationErrors);
        }

        Optional<ConnectCluster> optionalConnectCluster =
            connectClusterService.findByNamespaceAndNameOwner(ns, connectCluster);
        if (optionalConnectCluster.isEmpty()) {
            return HttpResponse.notFound();
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        ConnectCluster connectClusterToDelete = optionalConnectCluster.get();
        sendEventLog(connectClusterToDelete.getKind(), connectClusterToDelete.getMetadata(), ApplyStatus.deleted,
            connectClusterToDelete.getSpec(), null);

        connectClusterService.delete(connectClusterToDelete);
        return HttpResponse.noContent();
    }

    /**
     * List vault Kafka Connect clusters by namespace.
     *
     * @return A list of the available vault Kafka Connect clusters
     */
    @Get("/_/vaults")
    public List<ConnectCluster> listVaults(final String namespace) {
        return connectClusterService.findAllByNamespaceWrite(getNamespace(namespace))
            .stream()
            .filter(connectCluster -> StringUtils.hasText(connectCluster.getSpec().getAes256Key()))
            .toList();
    }

    /**
     * Encrypt a list of passwords.
     *
     * @param namespace      The namespace.
     * @param connectCluster The name of the Kafka Connect cluster.
     * @param passwords      The passwords to encrypt.
     * @return The encrypted password.
     */
    @Post("/{connectCluster}/vaults")
    public List<VaultResponse> vaultPassword(final String namespace, final String connectCluster,
                                             @Body final List<String> passwords) {
        final Namespace ns = getNamespace(namespace);

        final var validationErrors = new ArrayList<String>();
        if (!connectClusterService.isNamespaceAllowedForConnectCluster(ns, connectCluster)) {
            validationErrors.add(invalidConnectClusterNotAllowed(connectCluster));
        }

        validationErrors.addAll(connectClusterService.validateConnectClusterVault(ns, connectCluster));

        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(CONNECTOR_CLUSTER, connectCluster, validationErrors);
        }

        return connectClusterService.vaultPassword(ns, connectCluster, passwords);
    }
}
