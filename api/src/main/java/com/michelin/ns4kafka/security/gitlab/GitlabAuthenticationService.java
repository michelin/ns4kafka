package com.michelin.ns4kafka.security.gitlab;

import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.micronaut.http.HttpRequest.GET;

@Slf4j
@Singleton
public class GitlabAuthenticationService {

    @Inject
    GitlabApiClient gitlabApiClient;

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
        return gitlabApiClient.findUser(token)
                .firstElement()
                .map(stringObjectMap -> stringObjectMap.get("email").toString());
    }

    private Flowable<HttpResponse<List<Map<String, Object>>>> getPageAndNext(String token, int page){
        return gitlabApiClient.getGroupsPage(token, page)
                .concatMap(response -> {
                    log.debug("Called gitlab.com groups page {}/{}",page,response.header("X-Total-Pages"));
                    if(StringUtils.isEmpty(response.header("X-Next-Page"))){
                        return Flowable.just(response);
                    }else{
                        int nextPage = Integer.parseInt(response.header("X-Next-Page"));
                        return Flowable.just(response)
                                .concatWith(getPageAndNext(token, nextPage));
                    }
                });
    }
}
