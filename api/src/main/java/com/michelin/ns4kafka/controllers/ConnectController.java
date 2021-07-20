package com.michelin.ns4kafka.controllers;

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
    @Inject
    KafkaConnectService kafkaConnectService;

    @Get
    public List<Connector> list(String namespace) {
        return kafkaConnectService.findAllForNamespace(getNamespace(namespace));
    }

    @Get("/{connector}")
    public Optional<Connector> getConnector(String namespace, String connector) {
        return kafkaConnectService.findByName(getNamespace(namespace), connector);
    }

    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{connector}{?dryrun}")
    public HttpResponse<Void> deleteConnector(String namespace, String connector, @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);
        //check ownership
        if (!kafkaConnectService.isNamespaceOwnerOfConnect(ns, connector)) {
            throw new ResourceValidationException(List.of("Invalid value " + connector +
                     " for name: Namespace not OWNER of this connector"), "Connector",connector);
        }

        // exists ?
        Optional<Connector> optionalConnector = kafkaConnectService.findByName(ns, connector);
        if (optionalConnector.isEmpty())
            return HttpResponse.notFound();

        if (dryrun) {
            return HttpResponse.noContent();
        }

        //delete resource
        kafkaConnectService.delete(ns, optionalConnector.get());
        return HttpResponse.noContent();


    }

    @Post("{?dryrun}")
    public HttpResponse<Connector> apply(String namespace, @Valid @Body Connector connector, @QueryValue(defaultValue = "false") boolean dryrun) {

        Namespace ns = getNamespace(namespace);

        //check ownership
        if (!kafkaConnectService.isNamespaceOwnerOfConnect(ns, connector.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid value " + connector.getMetadata().getName() +
                     " for name: Namespace not OWNER of this connector"), connector.getKind(), connector.getMetadata().getName());
        }

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
                .state(Connector.TaskState.UNASSIGNED) //or else ?
                //.tasks(List.of(Tas))
                .build());

        Optional<Connector> existingConnector = kafkaConnectService.findByName(ns, connector.getMetadata().getName());
        if (existingConnector.isPresent() && existingConnector.get().equals(connector)) {
            return formatHttpResponse(existingConnector.get(), ApplyStatus.unchanged);
        }
        ApplyStatus status = ApplyStatus.created;
        if (existingConnector.isPresent()) {
            status = ApplyStatus.changed;
        }
        //dryrun checks
        if (dryrun) {
            return formatHttpResponse(connector, status);
        }
        //Create resource
        return formatHttpResponse(kafkaConnectService.createOrUpdate(ns, connector), status);
    }

    @Post("/_/import{?dryrun}")
    public List<Connector> importResources(String namespace, @QueryValue(defaultValue = "false") boolean dryrun) {

        Namespace ns = getNamespace(namespace);

        List<Connector> unsynchronizedConnectors = kafkaConnectService.listUnsynchronizedConnectors(ns);

        // Augment
        unsynchronizedConnectors.forEach(connector -> {
            connector.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
            connector.getMetadata().setCluster(ns.getMetadata().getCluster());
            connector.getMetadata().setNamespace(ns.getMetadata().getName());
        });

        if (dryrun) {
            return unsynchronizedConnectors;
        }

        List<Connector> synchronizedConnectors = unsynchronizedConnectors.stream()
                .map(connector -> kafkaConnectService.createOrUpdate(ns, connector))
                .collect(Collectors.toList());
        return synchronizedConnectors;
    }
}
