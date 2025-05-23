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

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.ReadTimeoutException;
import io.micronaut.retry.annotation.Retryable;
import java.util.List;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Gitlab API client. */
@Client(id = "gitlab")
public interface GitlabApiClient {
    /**
     * Get user groups.
     *
     * @param token The user token
     * @param page The current page to fetch groups
     * @return The groups
     */
    @Get("/api/v4/groups?min_access_level=10&sort=asc&page={page}&per_page=100")
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    Flux<HttpResponse<List<Map<String, Object>>>> getGroupsPage(
            @Header(value = "PRIVATE-TOKEN") String token, int page);

    /**
     * Find a user by given token.
     *
     * @param token The user token
     * @return The user information
     */
    @Get("/api/v4/user")
    @Retryable(
            delay = "${ns4kafka.retry.delay}",
            attempts = "${ns4kafka.retry.attempt}",
            multiplier = "${ns4kafka.retry.multiplier}",
            includes = ReadTimeoutException.class)
    Mono<Map<String, Object>> findUser(@Header(value = "PRIVATE-TOKEN") String token);
}
