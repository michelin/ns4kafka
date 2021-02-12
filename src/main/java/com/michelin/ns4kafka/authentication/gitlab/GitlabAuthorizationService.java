package com.michelin.ns4kafka.authentication.gitlab;

import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.*;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.micronaut.http.HttpRequest.GET;

@Singleton
public class GitlabAuthorizationService {

    private static final Logger LOG = LoggerFactory.getLogger(GitlabAuthorizationService.class);

    @Client("${micronaut.security.gitlab.url}")
    @Inject
    RxHttpClient httpClient;

    public GitlabAuthorizationService(){

    }

    public List<String> findAllGroups(String token){
        //TODO RxJava, need help here, spent too much time trying non blocking without success
        int nextPage=1;
        List<String> groups = new ArrayList<>();
        do {
            LOG.debug("Call gitlab for page "+nextPage);
            HttpResponse<List<Map<String,Object>>> response = httpClient.exchange(
                    GET("/api/v4/groups?min_access_level=10&page=" + nextPage).header("PRIVATE-TOKEN", token),
                    Argument.listOf(Argument.mapOf(String.class, Object.class)))
                    .firstElement()
                    .observeOn(Schedulers.io())
                    .blockingGet();
            groups.addAll(response.body()
                    .stream()
                    .map(item -> item.get("full_path").toString())
                    .collect(Collectors.toList())
            );
            if(response.header("X-Next-Page")==null || response.header("X-Next-Page").isEmpty()){
                nextPage=-1;//finito
            }else{
                nextPage = Integer.parseInt(response.header("X-Next-Page"));
            }
        }while (nextPage > 1);
        return groups;
    }

    public void rxFindAllGroups(String token){
        // Unused can't make this work
        Observable.range(1,99)
                .flatMap(integer ->
                        httpClient.exchange(GET("/api/v4/groups?min_access_level=10&page=" + integer)
                                .header("PRIVATE-TOKEN", token),
                                Argument.listOf(Argument.mapOf(String.class, Object.class)))
                                .firstElement().toObservable()
                )
                .takeUntil((Predicate<? super HttpResponse<List<Map<String, Object>>>>) response ->
                        response.header("X-Next-Page") != null && !response.header("X-Next-Page").isEmpty())
                .subscribe(response -> LOG.debug("TEST:"+
                        response.body()
                                .stream()
                                .map(item -> item.get("full_path").toString())
                                .collect(Collectors.joining(", "))
                ));

    }

    public Maybe<String> findUsername(String token){
        return httpClient.retrieve(
                GET("/api/v4/user").header("PRIVATE-TOKEN",token),
                Argument.mapOf(String.class,Object.class)
        ).firstElement().map(stringObjectMap -> stringObjectMap.get("email").toString());
    }

}
