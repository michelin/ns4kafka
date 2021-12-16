package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.models.KafkaUserResetPassword;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class UserTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    RxHttpClient client;

    private String token;

    @BeforeAll
    void init() {
        Namespace ns1 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns1")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-1"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user1")
                        .connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicTest.BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), TopicTest.BearerAccessRefreshToken.class).blockingFirst();

        token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1)).blockingFirst();
    }

    @Test
    void createAndUpdateUser() throws ExecutionException, InterruptedException {
        KafkaUserResetPassword response = client.retrieve(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/users/reset-password").bearerAuth(token), KafkaUserResetPassword.class).blockingFirst();

        Map<String, UserScramCredentialsDescription> mapUser = getAdminClient()
                .describeUserScramCredentials(List.of("user1")).all().get();
        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
                .describeClientQuotas(ClientQuotaFilter.containsOnly(
                        List.of(ClientQuotaFilterComponent.ofEntity("user", "user1")))
                ).entities().get();

        Assertions.assertNotNull(response.getSpec().getNewPassword());
        Assertions.assertTrue(mapUser.containsKey("user1"));
        Assertions.assertEquals(ScramMechanism.SCRAM_SHA_512, mapUser.get("user1").credentialInfos().get(0).mechanism());
        Assertions.assertEquals(4096, mapUser.get("user1").credentialInfos().get(0).iterations());

        Assertions.assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        Assertions.assertTrue(quotas.containsKey("producer_byte_rate"));
        Assertions.assertEquals(102400.0, quotas.get("producer_byte_rate"));
        Assertions.assertTrue(quotas.containsKey("consumer_byte_rate"));
        Assertions.assertEquals(102400.0, quotas.get("consumer_byte_rate"));

    }
}
