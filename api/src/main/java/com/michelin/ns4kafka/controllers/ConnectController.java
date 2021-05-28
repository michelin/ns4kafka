package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.KafkaConnectService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Tag(name = "Connects")
@Controller(value = "/api/namespaces/{namespace}/connects")
@ExecuteOn(TaskExecutors.IO)
public class ConnectController extends NamespacedResourceController {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectController.class);
    //TODO validate calls and forward to Connect REST API (sync ???)
    @Inject
    KafkaConnectService kafkaConnectService;
    
    @Inject
    AccessControlEntryService accessControlEntryService;

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
                    " for name: Namespace not OWNER of this connector"));
        }

        // exists ?
        Optional<Connector> optionalConnector = kafkaConnectService.findByName(ns, connector);
        if(optionalConnector.isEmpty())
            return HttpResponse.notFound();

        if (dryrun) {
            return HttpResponse.noContent();
        }

        //delete resource
        kafkaConnectService.delete(ns, optionalConnector.get());
        return HttpResponse.noContent();


    }

    @Post("{?dryrun}")
    public Connector apply(String namespace, @Valid @Body Connector connector, @QueryValue(defaultValue = "false") boolean dryrun) {

        Namespace ns = getNamespace(namespace);

        //check ownership
        if (!kafkaConnectService.isNamespaceOwnerOfConnect(ns, connector.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid value " + connector.getMetadata().getName() +
                    " for name: Namespace not OWNER of this connector"));
        }

        // Validate locally
        List<String> validationErrors = kafkaConnectService.validateLocally(ns, connector);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }

        // Validate against connect rest API /validate
        validationErrors = kafkaConnectService.validateRemotely(ns, connector);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }

        // Augment with server side fields
        connector.getMetadata().setCreationTimestamp(Date.from(Instant.now()));
        connector.getMetadata().setCluster(ns.getMetadata().getCluster());
        connector.getMetadata().setNamespace(ns.getMetadata().getName());
        connector.setStatus(Connector.ConnectorStatus.builder()
                .state(Connector.TaskState.UNASSIGNED) //or else ?
                //.tasks(List.of(Tas))
                .build());

        //dryrun checks
        if (dryrun) {
            return connector;
        }
        //Create resource
        return kafkaConnectService.createOrUpdate(ns, connector);
    }

    @Post("/_/synchronize")
    public List<Connector> synchronize(String namespace, @QueryValue(defaultValue = "false") boolean dryrun)
            throws ExecutionException, InterruptedException, TimeoutException {

        Namespace ns = getNamespace(namespace);

        if (ns == null)
            throw new ResourceNotFoundException();

        // Get ns4kfk access control entries for namespace 
        List<AccessControlEntry> accessControlEntries = accessControlEntryService.findAllGrantedToNamespace(ns);
        
        //  Get the list of connectors with prefixed accessControlEntry that exists in ns4kfk
        List<String> connectPatternToCreate = accessControlEntries
                .stream()
                .filter(accessControlEntry -> {
                            if (accessControlEntry.getSpec().getResourceType().equals(AccessControlEntry.ResourceType.CONNECT)
                                    && accessControlEntry.getSpec().getResourcePatternType().equals(AccessControlEntry.ResourcePatternType.PREFIXED)
                            ) {
                                return true;
                            }
                            return false;
                        }
                )
                .map(accessControlEntry -> accessControlEntry.getSpec().getResource())
                .collect(Collectors.toList());

        // Build Topic and Connect objects for topics to create in ns4kfk
        List<Connector> connectors = kafkaConnectService.buildConnectList(connectPatternToCreate, ns);
        

        // if dry run, do nothing
        if (dryrun) {
            return connectors;
        }

        connectors.stream().forEach(connector -> kafkaConnectService.createOrUpdate(ns, connector));

        return connectors;
    }

}
