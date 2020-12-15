package com.michelin.ns4kafka.authentication.gitlab;

import io.micronaut.context.annotation.Requires;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;

import static io.micronaut.http.HttpRequest.GET;

@Singleton
public class GitlabAuthorizationService {

    @Client("${micronaut.security.gitlab.url}")
    @Inject
    RxHttpClient httpClient;

    public GitlabAuthorizationService(){

    }

    public Maybe<List<Map<String,Object>>> findGroups(String token){
        return httpClient.retrieve(
                //TODO this will (eventually) fail because of gitlab pagination. Returns only page 1 for now.
                //https://docs.gitlab.com/ee/api/groups.html
                GET("/api/v4/groups?min_access_level=10").header("PRIVATE-TOKEN",token),
                Argument.listOf(Argument.mapOf(String.class,Object.class))
        ).firstElement();
    }
    public Maybe<String> findUsername(String token){
        return httpClient.retrieve(
                GET("/api/v4/user").header("PRIVATE-TOKEN",token),
                Argument.mapOf(String.class,Object.class)
        ).firstElement().map(stringObjectMap -> stringObjectMap.get("email").toString());
    }

}
