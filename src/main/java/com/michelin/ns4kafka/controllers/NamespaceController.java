package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

import javax.annotation.security.RolesAllowed;
import javax.naming.Name;
import java.util.Collections;
import java.util.List;

@Secured(SecurityRule.IS_ANONYMOUS)
@Controller("api/namespace")
public class NamespaceController {
    @Get
    public List<String> list(){
        return Collections.singletonList("1");
    }

    @Get("{namespace}")
    public String display(String namespace){
        return "1";
    }

    @Post("{namespace}")
    public HttpResponse create(String namespace){
        return HttpResponse.status(HttpStatus.CONFLICT,"Resource already exists. Use PUT instead.")
                .body("Use PUT\n");
    }
}
