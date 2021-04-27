package com.michelin.ns4kafka.cli.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ResourceDefinition;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.List;

@Client("${api.server}")
public interface ClusterResourceClient {

    @Post("/login")
    BearerAccessRefreshToken login(@Body UsernameAndPasswordRequest request);

    @Get("/api-resources")
    List<ResourceDefinition> listResourceDefinitions();

    @Delete("/api/{kind}/{resource}")
    void delete(@Header("Authorization") String token, String kind, String resource);

    @Post("/api/{kind}")
    Resource apply(@Header("Authorization") String token, String kind, @Body String json);

    @Get("/api/{kind}")
    List<Resource> list(@Header("Authorization") String token, String kind);

    @Introspected
    @Getter
    @Setter
    @Builder
    public static class UsernameAndPasswordRequest {
        private String username;
        private String password;
    }

    @Introspected
    @Getter
    @Setter
    public static class BearerAccessRefreshToken {
        private String username;
        private Collection<String> roles;

        @JsonProperty("access_token")
        private String accessToken;

        @JsonProperty("token_type")
        private String tokenType;

        @JsonProperty("expires_in")
        private Integer expiresIn;
    }
}
