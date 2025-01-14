/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.ns4kafka.security.auth.gitlab;

import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Gitlab authentication service.
 */
@Slf4j
@Singleton
public class GitlabAuthenticationService {
    @Inject
    GitlabApiClient gitlabApiClient;

    /**
     * Get all GitLab user groups.
     *
     * @param token The user token
     * @return The user groups
     */
    public Flux<String> findAllGroups(String token) {
        return getPageAndNext(token, 1)
            .flatMap(response -> Flux.fromStream(response.body()
                .stream()
                .map(stringObjectMap -> stringObjectMap.get("full_path").toString())));
    }

    /**
     * Get username of GitLab user.
     *
     * @param token The user token
     * @return The username
     */
    public Mono<String> findUsername(String token) {
        return gitlabApiClient.findUser(token)
            .map(stringObjectMap -> stringObjectMap.get("email").toString());
    }

    /**
     * Fetch all pages of GitLab user groups.
     *
     * @param token The user token
     * @param page  The current page to fetch
     * @return The user groups information
     */
    private Flux<HttpResponse<List<Map<String, Object>>>> getPageAndNext(String token, int page) {
        return gitlabApiClient.getGroupsPage(token, page)
            .concatMap(response -> {
                log.debug("Call GitLab groups page {}/{}.", page, response.header("X-Total-Pages"));

                if (StringUtils.isEmpty(response.header("X-Next-Page"))) {
                    return Flux.just(response);
                } else {
                    int nextPage = Integer.parseInt(response.header("X-Next-Page"));
                    return Flux.just(response)
                        .concatWith(getPageAndNext(token, nextPage));
                }
            });
    }
}
