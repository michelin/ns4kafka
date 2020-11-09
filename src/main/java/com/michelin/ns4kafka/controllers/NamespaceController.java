package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.security.ResourceSecurityPolicy;
import com.michelin.ns4kafka.models.security.TopicSecurityPolicy;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;

import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Secured(SecurityRule.IS_ANONYMOUS)
@Controller("/api/namespace")
public class NamespaceController {
    @Get
    public List<String> list(){
        return Collections.singletonList("1");
    }

    @Secured(SecurityRule.IS_AUTHENTICATED)
    @Get("{namespace}")
    public Namespace display(Authentication authentication,String namespace){
        return generateFakeNS(authentication, namespace);
    }

    @Post("{namespace}")
    public HttpResponse create(Namespace namespace){
        return HttpResponse.status(HttpStatus.CONFLICT,"Resource already exists. Use PUT instead.")
                .body("Use PUT\n");
    }

    private Namespace generateFakeNS(Authentication authentication, String name){
        Namespace n = new Namespace();
        n.setName(name);
        n.setAuthentication(authentication);
        n.setAdminLdapGroup("GP-FAKE-ADMIN");
        n.setQuotas(Map.of("cluster1","5Go","cluster2","2Go"));
        n.setPolicies(List.of(new TopicSecurityPolicy("f4m.*", ResourceSecurityPolicy.ResourcePatternType.PREFIXED,ResourceSecurityPolicy.SecurityPolicy.OWNER)));
        return n;
    }
}
