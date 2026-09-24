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
package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.enumation.Kind.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.model.Status;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.core.convert.exceptions.ConversionErrorException;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.exceptions.HttpClientException;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.server.exceptions.NotAllowedException;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthorizationException;
import jakarta.validation.ConstraintViolationException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ExceptionHandlerControllerTest {
    ExceptionHandlerController exceptionHandlerController = new ExceptionHandlerController();

    @Test
    void shouldHandleResourceValidationException() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.POST, "local"),
                new ResourceValidationException(TOPIC, "Name", List.of("Error1", "Error2")));
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY.getCode(), response.body().getCode());
        assertEquals("Resource validation failed", response.body().getMessage());

        assertEquals(TOPIC, response.body().getDetails().getKind());
        assertEquals("Name", response.body().getDetails().getName());
        assertEquals("Error1", response.body().getDetails().getCauses().getFirst());
        assertEquals("Error2", response.body().getDetails().getCauses().get(1));
    }

    @Test
    void shouldHandleConstraintViolationException() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.POST, "local"), new ConstraintViolationException(Set.of()));
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY.getCode(), response.body().getCode());
    }

    @Test
    void shouldHandleAuthorizationExceptionAndConvertToUnauthorized() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.POST, "local"), new AuthorizationException(null));
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.UNAUTHORIZED.getCode(), response.body().getCode());
    }

    @Test
    void shouldHandleAuthorizationExceptionAndConvertToForbidden() {
        AuthorizationException exception = new AuthorizationException(Authentication.build("user", Map.of()));
        HttpResponse<Status> response =
                exceptionHandlerController.error(HttpRequest.create(HttpMethod.POST, "local"), exception);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.FORBIDDEN.getCode(), response.body().getCode());
        assertEquals(HttpStatus.FORBIDDEN.getReason(), response.body().getMessage());
    }

    @Test
    void shouldHandleNotAllowedException() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.PUT, "/api/namespaces/ns1/topics"),
                new NotAllowedException("PUT", URI.create("/api/namespaces/ns1/topics"), Set.of("GET")));
        assertEquals(HttpStatus.METHOD_NOT_ALLOWED, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.METHOD_NOT_ALLOWED.getCode(), response.body().getCode());
    }

    @Test
    void shouldHandleAuthenticationException() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.POST, "local"), new AuthenticationException());
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.UNAUTHORIZED.getCode(), response.body().getCode());
    }

    @Test
    void shouldHandleHttpClientResponseException() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.POST, "local"),
                new HttpClientResponseException("Unknown Error", HttpResponse.status(520, "Unknown Error")));
        assertEquals(520, response.code());
        assertEquals("Unknown Error", response.reason());
        assertNotNull(response.body());
        assertEquals(520, response.body().getCode());
        assertEquals("Unknown Error", response.body().getReason());
        assertEquals("Remote service error", response.body().getMessage());
        assertEquals("Unknown Error", response.body().getDetails().getCauses().getFirst());
    }

    @Test
    void shouldHandleHttpClientException() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.POST, "local"),
                new HttpClientException("Connect Error: Connection refused"));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE.getCode(), response.body().getCode());
        assertEquals("Remote service error", response.body().getMessage());
        assertEquals(
                "Connect Error: Connection refused",
                response.body().getDetails().getCauses().getFirst());
    }

    @Test
    void shouldHandleConversionErrorException() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.GET, "/api/topics?phase=unknownPhase"),
                new ConversionErrorException(
                        Argument.of(Resource.Metadata.Phase.class, "phase"),
                        new IllegalArgumentException(
                                "No enum constant com.michelin.ns4kafka.model.Resource.Metadata.Phase.unknownPhase")));

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.BAD_REQUEST.getCode(), response.body().getCode());
        assertEquals("Bad request", response.body().getMessage());
        assertEquals(
                "No enum constant com.michelin.ns4kafka.model.Resource.Metadata.Phase.unknownPhase",
                response.body().getDetails().getCauses().getFirst());
    }

    @Test
    void shouldHandleAnyException() {
        HttpResponse<Status> response = exceptionHandlerController.error(
                HttpRequest.create(HttpMethod.POST, "local"), new Exception("Unexpected error"));

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatus());
        assertNotNull(response.body());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.getCode(), response.body().getCode());
        assertEquals("Internal server error", response.body().getMessage());
        assertEquals(
                "Unexpected error", response.body().getDetails().getCauses().getFirst());
    }

    @Test
    void shouldHandleAnyExceptionWithoutMessage() {
        HttpResponse<Status> response =
                exceptionHandlerController.error(HttpRequest.create(HttpMethod.POST, "local"), new Exception());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatus());
        assertNotNull(response.body());
        assertEquals("Internal server error", response.body().getMessage());
        assertNull(response.body().getDetails());
    }
}
