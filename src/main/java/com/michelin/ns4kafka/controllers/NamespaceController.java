package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.role.RoleBinding;
import com.michelin.ns4kafka.models.security.ResourceSecurityPolicy;
import com.michelin.ns4kafka.models.security.TopicSecurityPolicy;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;

import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Controller("/api/namespace")
public class NamespaceController {

    @Inject
    NamespaceRepository namespaceRepository;
    @Inject
    RoleBindingRepository roleBindingRepository;

    @Get("{namespace}/topics/{resourceType}/produce")
    public Namespace create(String namespace, String resourceType){
        Namespace n1 = new Namespace();
        n1.setName(namespace);
        n1.setOwner("f4m/admins");
        n1.setDiskQuota(5);
        n1.setPolicies(List.of(new TopicSecurityPolicy("f4m.*", ResourceSecurityPolicy.ResourcePatternType.PREFIXED,ResourceSecurityPolicy.SecurityPolicy.OWNER)));

        return namespaceRepository.createNamespace(n1);
    }

    @Get("init")
    public RoleBinding getRole(){
        Namespace n1 = new Namespace();
        n1.setName("rf4maze0");
        n1.setOwner("f4m/admins");
        n1.setDiskQuota(5);
        n1.setPolicies(List.of(new TopicSecurityPolicy("f4m.*", ResourceSecurityPolicy.ResourcePatternType.PREFIXED,ResourceSecurityPolicy.SecurityPolicy.OWNER)));

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
