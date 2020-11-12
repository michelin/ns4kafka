package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;

import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthorizationException;
import io.micronaut.security.rules.SecurityRule;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

@SecurityRequirement(name = "X-Gitlab-Token")
@Secured(SecurityRule.IS_AUTHENTICATED)
@Controller("/api/namespace")
public class NamespaceController {

    @Inject
    NamespaceRepository namespaceRepository;

    @Get
    public List<Namespace> list(){
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
