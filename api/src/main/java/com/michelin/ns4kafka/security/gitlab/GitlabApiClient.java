package com.michelin.ns4kafka.security.gitlab;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Flowable;

import java.util.List;
import java.util.Map;

import static io.micronaut.http.HttpRequest.GET;

@Client("${micronaut.security.gitlab.url}")
public interface GitlabApiClient {
    @Get("/api/v4/groups?min_access_level=10&sort=asc&page={page}")
    public Flowable<HttpResponse<List<Map<String, Object>>>> getGroupsPage(@Header(value = "PRIVATE-TOKEN") String token, int page);

    @Get("/api/v4/user")
    public Flowable<Map<String,Object>> findUser(@Header(value = "PRIVATE-TOKEN") String token);

}
