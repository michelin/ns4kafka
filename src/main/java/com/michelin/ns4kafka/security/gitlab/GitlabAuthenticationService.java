package com.michelin.ns4kafka.security.gitlab;

import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
@Singleton
public class GitlabAuthenticationService {
    @Inject
    GitlabApiClient gitlabApiClient;

    /**
     * Get all GitLab user groups
     * @param token The user token
     * @return The user groups
     */
    public Flowable<String> findAllGroups(String token){
        return getPageAndNext(token,1)
            .flatMap(response -> Flowable.fromIterable(
                response.body()
                        .stream()
                        .map(stringObjectMap -> stringObjectMap.get("full_path").toString())
                        .toList()
            )
        );
    }

    /**
     * Get username of GitLab user
     * @param token The user token
     * @return The username
     */
    public Maybe<String> findUsername(String token) {
        return gitlabApiClient.findUser(token)
            .firstElement()
            .map(stringObjectMap -> stringObjectMap.get("email").toString());
    }

    /**
     * Fetch all pages of GitLab user groups
     * @param token The user token
     * @param page The current page to fetch
     * @return The user groups information
     */
    private Flowable<HttpResponse<List<Map<String, Object>>>> getPageAndNext(String token, int page){
        return gitlabApiClient.getGroupsPage(token, page)
            .concatMap(response -> {
                log.debug("Call GitLab groups page {}/{}", page, response.header("X-Total-Pages"));

                if (StringUtils.isEmpty(response.header("X-Next-Page"))) {
                    return Flowable.just(response);
                } else {
                    int nextPage = Integer.parseInt(response.header("X-Next-Page"));
                    return Flowable.just(response)
                            .concatWith(getPageAndNext(token, nextPage));
                }
            });
    }
}
