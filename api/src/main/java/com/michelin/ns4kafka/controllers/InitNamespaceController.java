package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Connector;
import com.michelin.ns4kafka.security.ResourceBasedSecurityRule;
import com.michelin.ns4kafka.services.InitNamespaceService;
import com.michelin.ns4kafka.services.connect.KafkaConnectService;
import com.michelin.ns4kafka.services.connect.client.KafkaConnectClient;
import com.michelin.ns4kafka.services.connect.client.entities.ConnectorStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Tag(name = "Init")
@RolesAllowed(ResourceBasedSecurityRule.IS_ADMIN)
@Controller(value = "/api/namespaces/{namespace}/init")
public class InitNamespaceController extends NamespacedResourceController {
    
    @Inject
    InitNamespaceService initNamespaceService;

    @Get("{cluster}/{user}")
    public List<Object> init(String namespace, String cluster, String user) throws ExecutionException, InterruptedException, TimeoutException {
        List<Object> list = new ArrayList<>();
        // init namespace
        list.add(initNamespaceService.findNameSpaceByUser(namespace, cluster, user));
        // init role bindings
        list.add(initNamespaceService.findRoleBindingByUser(namespace, cluster));
        // init topic acls
        list.addAll(initNamespaceService.findAclsByUser(namespace, user, cluster));     
        return list;
    }
    
}
