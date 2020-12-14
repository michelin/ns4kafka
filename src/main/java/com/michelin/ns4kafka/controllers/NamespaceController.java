package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.*;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import com.michelin.ns4kafka.repositories.TopicRepository;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;

import io.micronaut.security.authentication.Authentication;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Controller("/api/namespaces")
public class NamespaceController {

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    RoleBindingRepository roleBindingRepository;
    @Inject
    TopicRepository topicRepository;


    @Get("init")
    public RoleBinding getRole(){
        Namespace n1 = new Namespace();
        n1.setName("rf4maze0");
        n1.setDiskQuota(5);
        n1.setPolicies(List.of(new TopicSecurityPolicy("f4m.", ResourceSecurityPolicy.ResourcePatternType.PREFIXED,ResourceSecurityPolicy.SecurityPolicy.OWNER)));

        Topic t1 = new Topic();
        t1.setName("f4m.topic1");
        t1.setCluster("MFG_AZE");
        topicRepository.create(t1);

        namespaceRepository.createNamespace(n1);
        RoleBinding rb = new RoleBinding("rf4maze0", "f4m/admins" );

        return roleBindingRepository.create(rb);
    }

    @Get
    public Collection<Namespace> list(){
        return namespaceRepository.findAll();
    }

    @Get("{namespace}")
    public Optional<Namespace> display(Authentication authentication, String namespace){
        return namespaceRepository.findByName(namespace);
    }

    @Post("{namespace}")
    public HttpResponse create(Namespace namespace){

        return HttpResponse.status(HttpStatus.CONFLICT,"Resource already exists. Use PUT instead.")
                .body("Use PUT\n");
    }

}
