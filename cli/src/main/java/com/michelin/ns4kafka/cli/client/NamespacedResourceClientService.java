package com.michelin.ns4kafka.cli.client;

import com.michelin.ns4kafka.cli.models.Resource;
import com.michelin.ns4kafka.cli.models.SchemaCompatibility;
import com.michelin.ns4kafka.cli.services.ConfigService;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;

@Singleton
public class NamespacedResourceClientService {
    @Inject
    HttpClient client;

    @Inject
    ConfigService configService;

    public HttpResponse<Object> delete(String namespace, String kind, String resourceName, String token, boolean dryRun) {
        String uri = "/api/namespaces/" + namespace + "/" + kind + "/" + resourceName + "?dryrun=" + dryRun;

        return client.toBlocking().exchange(HttpRequest.DELETE(configService.getCurrentContextInfos().getContext().getApi() +
                uri).header("Authorization", token));
    }

    public HttpResponse<Resource> apply(String namespace, String kind, String token, Resource body, boolean dryRun) {
        String uri = "/api/namespaces/" + namespace + "/" + kind + "/" + "?dryrun=" + dryRun;

        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                uri, body).header("Authorization", token), Resource.class);
    }

    public List<Resource> list(String namespace, String kind, String token) {
        String uri = "/api/namespaces/" + namespace + "/" + kind;

        return client.toBlocking().exchange(HttpRequest.GET(configService.getCurrentContextInfos().getContext().getApi() +
                uri).header("Authorization", token), Argument.listOf(Resource.class)).body();
    }

    public Resource get(String namespace, String kind, String resourceName, String token) {
        String uri = "/api/namespaces/" + namespace + "/" + kind + "/" + resourceName;

        return client.toBlocking().exchange(HttpRequest.GET(configService.getCurrentContextInfos().getContext().getApi() +
                uri).header("Authorization", token), Resource.class).body();
    }

    public List<Resource> importResources(String namespace, String kind, String token, boolean dryRun) {
        String uri = "/api/namespaces/" + namespace + "/" + kind + "/_/import" + "?dryrun=" + dryRun;

        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                uri, null).header("Authorization", token), Argument.listOf(Resource.class)).body();
    }

    public Resource deleteRecords(String token, String namespace, String topic, boolean dryRun) {
        String uri = "/api/namespaces/" + namespace + "/topics/" + topic + "/delete-records" + "?dryrun=" + dryRun;

        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                uri, null).header("Authorization", token), Resource.class).body();
    }

    public Resource resetOffsets(String token, String namespace, String consumerGroupName, Resource body, boolean dryRun) {
        String uri = "/api/namespaces/" + namespace + "/consumer-groups/" + consumerGroupName + "/reset" + "?dryrun=" + dryRun;

        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                uri, body).header("Authorization", token), Resource.class).body();
    }

    public Resource changeConnectorState(String namespace, String connector, Resource body, String token) {
        String uri = "/api/namespaces/" + namespace + "/connects/" + connector + "/change-state";

        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                uri, body).header("Authorization", token), Resource.class).body();
    }

    public Resource changeSchemaCompatibility(String namespace, String subject, Map<String, SchemaCompatibility> compatibility, String token) {
        String uri = "/api/namespaces/" + namespace + "/schemas/" + subject + "/config";

        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                uri, compatibility).header("Authorization", token), Resource.class).body();
    }

    public Resource resetPassword(String namespace, String user, String token) {
        String uri = "/api/namespaces/" + namespace + "/users/" + user + "/reset-password";

        return client.toBlocking().exchange(HttpRequest.POST(configService.getCurrentContextInfos().getContext().getApi() +
                uri, null).header("Authorization", token), Resource.class).body();
    }
}
