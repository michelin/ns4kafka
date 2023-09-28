package com.michelin.ns4kafka.controllers.connect;

import com.michelin.ns4kafka.controllers.generic.NonNamespacedResourceController;
import com.michelin.ns4kafka.models.connect.cluster.ConnectCluster;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.ConnectClusterService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import reactor.core.publisher.Flux;

import jakarta.annotation.security.RolesAllowed;

@Tag(name = "Connect Clusters", description = "Manage the Kafka Connect clusters.")
@Controller(value = "/api/connect-clusters")
@ExecuteOn(TaskExecutors.IO)
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
public class ConnectClusterNonNamespacedController extends NonNamespacedResourceController {
    @Inject
    ConnectClusterService connectClusterService;

    /**
     * List Kafka Connect clusters
     * @return A list of Kafka Connect clusters
     */
    @Get("{?all}")
    public Flux<ConnectCluster> listAll(@QueryValue(defaultValue = "false") boolean all) {
        return connectClusterService.findAll(all);
    }
}
