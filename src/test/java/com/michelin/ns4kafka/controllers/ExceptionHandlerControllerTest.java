package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthorizationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolationException;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ExceptionHandlerControllerTest {

    ExceptionHandlerController exceptionHandlerController = new ExceptionHandlerController();

    @Test
    void resourceValidationError() {
        var response = exceptionHandlerController.error(HttpRequest.create(HttpMethod.POST, "local")
                                                      ,new ResourceValidationException(List.of("Error1", "Error2"),"Topic", "Name"));
        var status = response.body();

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, response.getStatus());
        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY.getCode(), status.getCode());

        Assertions.assertEquals("Topic", status.getDetails().getKind());
        Assertions.assertEquals("Name", status.getDetails().getName());
        Assertions.assertEquals("Error1", status.getDetails().getCauses().get(0));
        Assertions.assertEquals("Error2", status.getDetails().getCauses().get(1));
    }

    @Test
    void constraintViolationError() {
        var response = exceptionHandlerController.error(HttpRequest.create(HttpMethod.POST, "local")
                                                      ,new ConstraintViolationException(Set.of()));
        var status = response.body();

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, response.getStatus());
        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY.getCode(), status.getCode());
    }

    @Test
    void authorizationUnauthorizedError() {
        var response = exceptionHandlerController.error(HttpRequest.create(HttpMethod.POST, "local")
                                                      ,new AuthorizationException(null));
        var status = response.body();

        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, response.getStatus());
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED.getCode(), status.getCode());
    }

    @Test
    void authorizationForbiddenError() {
        var response = exceptionHandlerController.error(HttpRequest.create(HttpMethod.POST, "local")
                                                      ,new AuthorizationException(Authentication.build("user", Map.of())));
        var status = response.body();

        Assertions.assertEquals(HttpStatus.FORBIDDEN, response.getStatus());
        Assertions.assertEquals(HttpStatus.FORBIDDEN.getCode(), status.getCode());
    }

    @Test
    void authenticationError() {
        var response = exceptionHandlerController.error(HttpRequest.create(HttpMethod.POST, "local")
                                                      ,new AuthenticationException());
        var status = response.body();

        Assertions.assertEquals(HttpStatus.UNAUTHORIZED, response.getStatus());
        Assertions.assertEquals(HttpStatus.UNAUTHORIZED.getCode(), status.getCode());
    }

    @Test
    void anyError() {
        var response = exceptionHandlerController.error(HttpRequest.create(HttpMethod.POST, "local")
                                                      ,new Exception());
        var status = response.body();

        Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatus());
        Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.getCode(), status.getCode());
    }
}
