package com.michelin.ns4kafka;

import io.micronaut.openapi.annotation.OpenAPIInclude;
import io.micronaut.runtime.Micronaut;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.tags.Tag;

@SecurityScheme(name = "JWT",
        type = SecuritySchemeType.HTTP,
        scheme = "bearer",
        bearerFormat = "JWT")
@OpenAPIDefinition(
        security = @SecurityRequirement(name = "JWT"),
        info = @Info(
                title = "Ns4Kafka",
                version = "0.1",
                description = "Getting started with REST APIs."
        )
)
@OpenAPIInclude(
        classes = { io.micronaut.security.endpoints.LoginController.class },
        tags = @Tag(name = "_Security", description = "All the login endpoints.")
)
public class Application {

    public static void main(String[] args) {
        RxJavaPlugins.setErrorHandler(throwable -> {});
        Micronaut.run(Application.class, args);
    }
}
