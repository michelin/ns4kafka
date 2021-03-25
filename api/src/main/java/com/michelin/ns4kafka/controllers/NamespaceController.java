package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.NamespaceService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;

@Tag(name = "Namespaces")
@Controller("/api/namespaces")
public class NamespaceController {

    @Inject
    NamespaceService namespaceService;

    @Post
    public Namespace apply(Namespace namespace) {
        return null;
    }

    @Delete("/{namespace}")
    public HttpResponse delete(String namespace){
        return null;
    }

}
