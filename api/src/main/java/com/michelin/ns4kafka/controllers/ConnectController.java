package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Error;
import io.micronaut.http.annotation.*;
import io.micronaut.http.hateoas.JsonError;
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
public class ConnectController {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectController.class);
    //TODO validate calls and forward to Connect REST API (sync ???)
    @Inject
    KafkaConnectService kafkaConnectService;
    @Inject
    NamespaceRepository namespaceRepository;

    @Get
    public List<Connector> list(String namespace) {
        return kafkaConnectService.findByNamespace(namespace);
    }

    @Get("/{connector}")
    public Optional<Connector> getConnector(String namespace, String connector) {
        return kafkaConnectService.findByName(namespace, connector);
    }

    @Status(HttpStatus.NO_CONTENT)
    @Delete("/{connector}")
    public HttpResponse deleteConnector(String namespace, String connector) {
        //check ownership
        if (!kafkaConnectService.isNamespaceOwnerOfConnect(namespace, connector)) {
            throw new ResourceValidationException(List.of("Invalid value " + connector +
                    " for name: Namespace not OWNER of this connector"));
        }
        //delete resource
        return kafkaConnectService.delete(namespace, connector);

    }

    @Post
    public Connector apply(String namespace, @Valid @Body Connector connector) {

        Namespace ns = namespaceRepository.findByName(namespace).get();

        //check ownership
        if (!kafkaConnectService.isNamespaceOwnerOfConnect(namespace, connector.getMetadata().getName())) {
            throw new ResourceValidationException(List.of("Invalid value " + connector.getMetadata().getName() +
                    " for name: Namespace not OWNER of this connector"));
        }

        // Validate locally
        List<String> validationErrors = kafkaConnectService.validateLocally(namespace, connector);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }

        // Validate against connect rest API /validate
        validationErrors = kafkaConnectService.validateRemotely(namespace, connector);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        //Create resource
        return kafkaConnectService.createOrUpdate(namespace, connector);
    }

    //TODO move elsewhere
    public static class ConnectCreationException extends RuntimeException {
        public ConnectCreationException(Throwable e) {
            super(e);
        }
    }

    //TODO move elsewhere
    @Error(global = true)
    public HttpResponse<JsonError> validationExceptionHandler(HttpRequest request, ConnectCreationException e) {
        return HttpResponse.badRequest()
                .body(new JsonError(e.getMessage()));
    }

}
