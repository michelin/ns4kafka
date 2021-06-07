package com.michelin.ns4kafka.integration;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import lombok.Getter;
import lombok.Setter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.models.AccessControlEntry.AccessControlEntrySpec;
import com.michelin.ns4kafka.models.AccessControlEntry.Permission;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourcePatternType;
import com.michelin.ns4kafka.models.AccessControlEntry.ResourceType;
import com.michelin.ns4kafka.models.Namespace.NamespaceSpec;
import com.michelin.ns4kafka.models.RoleBinding.Role;
import com.michelin.ns4kafka.models.RoleBinding.RoleBindingSpec;
import com.michelin.ns4kafka.models.RoleBinding.Verb;
import com.michelin.ns4kafka.models.Topic.TopicSpec;
import com.michelin.ns4kafka.validation.TopicValidator;

@MicronautTest
public class NamespaceReadAccessToTopic {

    @Inject
    @Client("/")
    RxHttpClient client;



    @Test
    void unauthorizedModifications() {

        Namespace ns1 = Namespace.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1")
                      .cluster("test-cluster")
                      .build())
            .spec(NamespaceSpec.builder()
                  .kafkaUser("user1")
                  .connectClusters(List.of("test-connect"))
                  .topicValidator(TopicValidator.makeDefault())
                  .build())
            .build();

        RoleBinding rb1 = RoleBinding.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-rb")
                      .namespace("ns1")
                      .build())
            .spec(RoleBindingSpec.builder()
                  .role(Role.builder()
                        .resourceTypes(List.of("topics"))
                        .verbs(List.of(Verb.POST))
                        .build())
                  .build())
            .build();

        Namespace ns2 = Namespace.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns2")
                      .cluster("test-cluster")
                      .build())
            .spec(NamespaceSpec.builder()
                  .kafkaUser("user2")
                  .connectClusters(List.of("test-connect"))
                  .topicValidator(TopicValidator.makeDefault())
                  .build())
            .build();

        RoleBinding rb2 = RoleBinding.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns2-rb")
                      .namespace("ns2")
                      .build())
            .spec(RoleBindingSpec.builder()
                  .role(Role.builder()
                        .resourceTypes(List.of("topics"))
                        .verbs(List.of(Verb.POST))
                        .build())
                  .build())
            .build();

        AccessControlEntry acl1 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-acl1")
                      .namespace("ns1")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.TOPIC)
                  .resource("ns1-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.OWNER)
                  .grantedTo("ns1")
                  .build())
            .build();

        AccessControlEntry acl2 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns2-acl1")
                      .namespace("ns2")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.TOPIC)
                  .resource("ns2-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.OWNER)
                  .grantedTo("ns2")
                  .build())
            .build();

        AccessControlEntry acl3 = AccessControlEntry.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-acl2")
                      .namespace("ns1")
                      .build())
            .spec(AccessControlEntrySpec.builder()
                  .resourceType(ResourceType.TOPIC)
                  .resource("ns1-")
                  .resourcePatternType(ResourcePatternType.PREFIXED)
                  .permission(Permission.READ)
                  .grantedTo("ns2")
                  .build())
            .build();


        Topic t1 = Topic.builder()
            .metadata(ObjectMeta.builder()
                      .name("ns1-topic1")
                      .namespace("ns1")
                      .build())
            .spec(TopicSpec.builder()
                  .partitions(3)
                  .replicationFactor(3)
                  .configs(Map.of("cleanup.policy", "delete"))
                  .build())
            .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin","admin");
        HttpResponse<BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), BearerAccessRefreshToken.class).blockingFirst();
        String token = response.getBody().get().getAccessToken();


        client.toBlocking().exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces").bearerAuth(token).body(ns1));
        client.exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces/ns1/role-bindings").bearerAuth(token).body(rb1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces").bearerAuth(token).body(ns2)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces/ns2/role-bindings").bearerAuth(token).body(rb2)).blockingFirst();

        client.exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces/ns1/alcs").bearerAuth(token).body(acl1)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces/ns1/alcs").bearerAuth(token).body(acl3)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces/ns2/alcs").bearerAuth(token).body(acl2)).blockingFirst();

        Assertions.assertEquals(HttpStatus.OK,client.exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces/ns1/topics").bearerAuth(token).body(t1)).blockingFirst().getStatus());
        Topic t1bis = t1;
        t1bis.setSpec(TopicSpec.builder()
                                 .partitions(3)
                                 .replicationFactor(3)
                                 .configs(Map.of("cleanup.policy", "delete",
                                                 "min.insync.replicas", "2"))
                                 .build());

        Assertions.assertEquals(HttpStatus.BAD_REQUEST,client.exchange(HttpRequest.create(HttpMethod.POST,"api/namespaces/ns2/topics").bearerAuth(token).body(t1bis)).blockingFirst().getStatus());
    }

    @Introspected
    @Getter
    @Setter
    public class BearerAccessRefreshToken {
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
