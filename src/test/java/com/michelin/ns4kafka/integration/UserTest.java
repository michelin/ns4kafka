package com.michelin.ns4kafka.integration;

import com.michelin.ns4kafka.models.KafkaUserResetPassword;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.Status;
import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.services.executors.UserAsyncExecutor;
import com.michelin.ns4kafka.validation.TopicValidator;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.rxjava3.http.client.Rx3HttpClient;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
public class UserTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    Rx3HttpClient client;

    @Inject
    List<UserAsyncExecutor> userAsyncExecutors;

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
        Namespace ns2 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns2")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-2"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user2")
                        .connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();
        Namespace ns3 = Namespace.builder()
                .metadata(ObjectMeta.builder()
                        .name("ns3")
                        .cluster("test-cluster")
                        .labels(Map.of("support-group", "LDAP-GROUP-3"))
                        .build())
                .spec(Namespace.NamespaceSpec.builder()
                        .kafkaUser("user3")
                        .connectClusters(List.of("test-connect"))
                        .topicValidator(TopicValidator.makeDefaultOneBroker())
                        .build())
                .build();

        ResourceQuota rqNs2 = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .name("rqNs2")
                        .namespace("ns2")
                        .build())
                .spec(Map.of(
                        ResourceQuota.ResourceQuotaSpecKey.USER_PRODUCER_BYTE_RATE.getKey(), "204800.0",
                        ResourceQuota.ResourceQuotaSpecKey.USER_CONSUMER_BYTE_RATE.getKey(), "409600.0"))
                .build();

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicTest.BearerAccessRefreshToken> response = client.exchange(HttpRequest.POST("/login", credentials), TopicTest.BearerAccessRefreshToken.class).blockingFirst();

        token = response.getBody().get().getAccessToken();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1)).blockingFirst();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns2)).blockingFirst();
        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns2/resource-quotas").bearerAuth(token).body(rqNs2)).blockingFirst();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns3)).blockingFirst();

        //force User Sync
        userAsyncExecutors.forEach(UserAsyncExecutor::run);

    }

    @Test
    void checkDefaultQuotas() throws ExecutionException, InterruptedException {
        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
                .describeClientQuotas(ClientQuotaFilter.containsOnly(
                        List.of(ClientQuotaFilterComponent.ofEntity("user", "user1")))
                ).entities().get();

        Assertions.assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        Assertions.assertTrue(quotas.containsKey("producer_byte_rate"));
        Assertions.assertEquals(102400.0, quotas.get("producer_byte_rate"));
        Assertions.assertTrue(quotas.containsKey("consumer_byte_rate"));
        Assertions.assertEquals(102400.0, quotas.get("consumer_byte_rate"));
    }
    @Test
    void checkCustomQuotas() throws ExecutionException, InterruptedException {
        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
                .describeClientQuotas(ClientQuotaFilter.containsOnly(
                        List.of(ClientQuotaFilterComponent.ofEntity("user", "user2")))
                ).entities().get();

        Assertions.assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        Assertions.assertTrue(quotas.containsKey("producer_byte_rate"));
        Assertions.assertEquals(204800.0, quotas.get("producer_byte_rate"));
        Assertions.assertTrue(quotas.containsKey("consumer_byte_rate"));
        Assertions.assertEquals(409600.0, quotas.get("consumer_byte_rate"));
    }
    @Test
    void checkUpdateQuotas() throws ExecutionException, InterruptedException {
        // Update the namespace user quotas
        ResourceQuota rq3 = ResourceQuota.builder()
                .metadata(ObjectMeta.builder()
                        .name("rqNs3")
                        .namespace("ns3")
                        .build())
                .spec(Map.of(
                        ResourceQuota.ResourceQuotaSpecKey.USER_PRODUCER_BYTE_RATE.getKey(), "204800.0",
                        ResourceQuota.ResourceQuotaSpecKey.USER_CONSUMER_BYTE_RATE.getKey(), "409600.0"))
                .build();

        client.exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns3/resource-quotas").bearerAuth(token).body(rq3)).blockingFirst();

        // Force user sync to force the quota update
        userAsyncExecutors.forEach(UserAsyncExecutor::run);

        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
                .describeClientQuotas(ClientQuotaFilter.containsOnly(
                        List.of(ClientQuotaFilterComponent.ofEntity("user", "user3")))
                ).entities().get();

        Assertions.assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        Assertions.assertTrue(quotas.containsKey("producer_byte_rate"));
        Assertions.assertEquals(204800.0, quotas.get("producer_byte_rate"));
        Assertions.assertTrue(quotas.containsKey("consumer_byte_rate"));
        Assertions.assertEquals(409600.0, quotas.get("consumer_byte_rate"));
    }

    @Test
    void createAndUpdateUserForceTest() throws ExecutionException, InterruptedException {
        KafkaUserResetPassword response = client.retrieve(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/users/user1/reset-password").bearerAuth(token), KafkaUserResetPassword.class).blockingFirst();

        Map<String, UserScramCredentialsDescription> mapUser = getAdminClient()
                .describeUserScramCredentials(List.of("user1")).all().get();

        Assertions.assertNotNull(response.getSpec().getNewPassword());
        Assertions.assertTrue(mapUser.containsKey("user1"));
        Assertions.assertEquals(ScramMechanism.SCRAM_SHA_512, mapUser.get("user1").credentialInfos().get(0).mechanism());
        Assertions.assertEquals(4096, mapUser.get("user1").credentialInfos().get(0).iterations());
    }

    @Test
    void updateUserFail_NotMatching() {
        HttpClientResponseException exception = Assertions.assertThrows(HttpClientResponseException.class, () -> client.retrieve(HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/users/user2/reset-password").bearerAuth(token), KafkaUserResetPassword.class).blockingFirst());

        Assertions.assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        Assertions.assertEquals("Invalid user user2 : Doesn't belong to namespace ns1", exception.getResponse().getBody(Status.class).get().getDetails().getCauses().get(0));
    }
}