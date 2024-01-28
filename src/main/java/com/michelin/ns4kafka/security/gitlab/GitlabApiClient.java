package com.michelin.ns4kafka.security.gitlab;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Gitlab API client.
 */
@Client("${micronaut.security.gitlab.url}")
public interface GitlabApiClient {
    /**
     * Get user groups.
     *
     * @param token The user token
     * @param page  The current page to fetch groups
     * @return The groups
     */
    @Get("/api/v4/groups?min_access_level=10&sort=asc&page={page}&per_page=100")
    Flux<HttpResponse<List<Map<String, Object>>>> getGroupsPage(@Header(value = "PRIVATE-TOKEN") String token,
                                                                int page);

    /**
     * Get user subgroups under given group.
     *
     * @param token  The user token
     * @param parent The parent group under which to fetch groups
     * @param page   The current page to fetch groups
     * @return The groups
     */
    @Get("/api/v4/groups/{parent}/subgroups?min_access_level=10&sort=asc&page={page}&per_page=100")
    Flux<HttpResponse<List<Map<String, Object>>>> getGroupsPage(@Header(value = "PRIVATE-TOKEN") String token,
                                                                String parent,
                                                                int page);

    /**
     * Find a user by given token.
     *
     * @param token The user token
     * @return The user information
     */
    @Get("/api/v4/user")
    Mono<Map<String, Object>> findUser(@Header(value = "PRIVATE-TOKEN") String token);
}
