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
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Tag(name = "Namespaces")
@Controller("/api/namespaces")
public class NamespaceController {

    @Inject
    NamespaceRepository namespaceRepository;

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
                .body("Use PUT");
    }

}
