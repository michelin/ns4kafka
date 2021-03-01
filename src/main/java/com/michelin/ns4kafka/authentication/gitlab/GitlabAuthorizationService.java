package com.michelin.ns4kafka.authentication.gitlab;

import io.micronaut.core.type.Argument;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.micronaut.http.HttpRequest.GET;

@Singleton
public class GitlabAuthorizationService {

    private static final Logger LOG = LoggerFactory.getLogger(GitlabAuthorizationService.class);

    @Client("${micronaut.security.gitlab.url}")
    @Inject
    RxHttpClient httpClient;

    public Flowable<String> findAllGroups(String token){
        return getPageAndNext(token,1)
                .flatMap(response -> Flowable.fromIterable(
                        response.body()
                                .stream()
                                .map(stringObjectMap -> stringObjectMap.get("full_path").toString())
                                .collect(Collectors.toList())
                )
        );

    }

    public Maybe<String> findUsername(String token){
        return httpClient.retrieve(
                GET("/api/v4/user").header("PRIVATE-TOKEN",token),
                Argument.mapOf(String.class,Object.class)
        ).firstElement().map(stringObjectMap -> stringObjectMap.get("email").toString());
    }

    public Flowable<HttpResponse<List<Map<String, Object>>>> singlePage(String token, int page){
        return httpClient.exchange(
                GET("/api/v4/groups?min_access_level=10&sort=asc&page=" + page)
                        .header("PRIVATE-TOKEN", token),
                Argument.listOf(Argument.mapOf(String.class, Object.class)));
    }

    public Flowable<HttpResponse<List<Map<String, Object>>>> getPageAndNext(String token, int page){
        return singlePage(token, page)
                .concatMap(response -> {
                    LOG.debug("Called gitlab.com groups page "+page+"/"+response.header("X-Total-Pages"));
                    if(StringUtils.isEmpty(response.header("X-Next-Page"))){
                        return Flowable.just(response);
                    }else{
                        int nextPage = Integer.parseInt(response.header("X-Next-Page"));
                        return Flowable.just(response)
                                .concatWith(getPageAndNext(token, nextPage));
                    }
                });
    }

    /*
    public List<String> findAllGroups(String token){
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
    */
}
