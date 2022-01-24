package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.models.ApiResource;
import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.services.ConfigService;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class ClusterResourceClientService {
    @Inject
    HttpClient client;

    @Inject
    ConfigService configService;

    public BearerAccessRefreshToken login(UsernameAndPasswordRequest request) {
        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                "/login", request), BearerAccessRefreshToken.class).body();
    }

    public UserInfoResponse tokenInfo(String token) {
        return client.toBlocking().exchange(HttpRequest.GET(configService.getCurrentContextInfos().getContext().getApi() +
                "/token_info").header("Authorization", token), UserInfoResponse.class).body();
    }

    public List<ApiResource> listResourceDefinitions(String token) {
        return client.toBlocking().exchange(HttpRequest.GET(configService.getCurrentContextInfos().getContext().getApi() +
                "/api-resources").header("Authorization", token), Argument.listOf(ApiResource.class)).body();
    }

    public List<ApiResource> delete(String token, String kind, String resource, boolean dryRun) {
        String uri = "/api/" + kind + "/" + resource + "?dryrun=" + dryRun;

        return client.toBlocking().exchange(HttpRequest.DELETE(configService.getCurrentContextInfos().getContext().getApi() +
                        uri).header("Authorization", token), Argument.listOf(ApiResource.class)).body();
    }

    public HttpResponse<Resource> apply(String token, String kind, Resource body, boolean dryRun) {
        String uri = "/api/" + kind + "?dryrun=" + dryRun;

        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                uri, body).header("Authorization", token), Resource.class);
    }

    public List<Resource> list(String token, String kind) {
        String uri = "/api/" + kind;

        return client.toBlocking().exchange(HttpRequest.GET(configService.getCurrentContextInfos().getContext().getApi() +
                uri).header("Authorization", token), Argument.listOf(Resource.class)).body();
    }

    public Resource get(String token, String kind, String resource) {
        String uri = "/api/" + kind + "/" + resource;

        return client.toBlocking().exchange(HttpRequest.GET(configService.getCurrentContextInfos().getContext().getApi() +
                uri).header("Authorization", token), Resource.class).body();
    }
}
