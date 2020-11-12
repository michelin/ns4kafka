package com.michelin.ns4kafka.authentication.gitlab;

import io.micronaut.core.type.Argument;
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
        HttpRequest<?> req = GET("/api/v4/groups").header("PRIVATE-TOKEN",token);
        Flowable flow = httpClient.retrieve(req,Argument.listOf(Map.class),Argument.of(HttpClientResponseException.class));
        return flow.firstElement();
    }

}
