package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
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
import java.util.List;
import java.util.Optional;

@Tag(name = "Connects")
@Controller(value = "/api/namespaces/{namespace}/connects")
@ExecuteOn(TaskExecutors.IO)
public class ConnectController extends NamespacedResourceController {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectController.class);
    //TODO validate calls and forward to Connect REST API (sync ???)
    @Inject
    KafkaConnectService kafkaConnectService;

    @Get
    public List<Connector> list(String namespace) {
        return kafkaConnectService.list(getNamespace(namespace));
    }

    @Get("/{connector}")
    public Optional<Connector> getConnector(String namespace, String connector) {
        return kafkaConnectService.findByName(getNamespace(namespace), connector);
    }

    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{connector}")
    public HttpResponse deleteConnector(String namespace, String connector) {
        Namespace ns = getNamespace(namespace);
        //check ownership
        if (!kafkaConnectService.isNamespaceOwnerOfConnect(ns, connector)) {
            throw new ResourceValidationException(List.of("Invalid value " + connector +
                    " for name: Namespace not OWNER of this connector"));
        }
        //delete resource
        return kafkaConnectService.delete(ns, connector);

    }

    @Post
    public Connector apply(String namespace, @Valid @Body Connector connector) {

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
        //Create resource
        return kafkaConnectService.createOrUpdate(ns, connector);
    }

}
