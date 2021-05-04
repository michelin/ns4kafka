package com.michelin.ns4kafka.cli.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.ApiResource;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.annotation.*;
import io.micronaut.http.client.annotation.Client;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.List;

@Client("${kafkactl.api}")
public interface ClusterResourceClient {

    @Post("/login")
    BearerAccessRefreshToken login(@Body UsernameAndPasswordRequest request);

    @Get("/token_info")
    UserInfoResponse tokenInfo(@Header("Authorization") String token);

    @Get("/api-resources")
    List<ApiResource> listResourceDefinitions();

    @Delete("/api/{kind}/{resource}{?dryrun}")
    void delete(@Header("Authorization") String token, String kind, String resource, @QueryValue boolean dryrun);

    @Post("/api/{kind}{?dryrun}")
    Resource apply(@Header("Authorization") String token, String kind, @Body Resource json, @QueryValue boolean dryrun);

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

    @Introspected
    @Getter
    @Setter
    public static class UserInfoResponse {
        private boolean active;
        private String username;
        private long exp;
    }
}
