package com.michelin.ns4kafka.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
@Property(name = "micronaut.security.gitlab.enabled", value = "false")
class UserTest extends AbstractIntegrationTest {
    @Inject
    @Client("/")
    HttpClient client;

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

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin");
        HttpResponse<TopicTest.BearerAccessRefreshToken> response = client.toBlocking()
            .exchange(HttpRequest.POST("/login", credentials), TopicTest.BearerAccessRefreshToken.class);

        token = response.getBody().get().getAccessToken();

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns1));

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns2));

        ResourceQuota rqNs2 = ResourceQuota.builder()
            .metadata(ObjectMeta.builder()
                .name("rqNs2")
                .namespace("ns2")
                .build())
            .spec(Map.of(
                ResourceQuota.ResourceQuotaSpecKey.USER_PRODUCER_BYTE_RATE.getKey(), "204800.0",
                ResourceQuota.ResourceQuotaSpecKey.USER_CONSUMER_BYTE_RATE.getKey(), "409600.0"))
            .build();

        client.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns2/resource-quotas").bearerAuth(token).body(rqNs2));

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

        client.toBlocking()
            .exchange(HttpRequest.create(HttpMethod.POST, "/api/namespaces").bearerAuth(token).body(ns3));

        //force User Sync
        userAsyncExecutors.forEach(UserAsyncExecutor::run);

    }

    @Test
    void checkDefaultQuotas() throws ExecutionException, InterruptedException {
        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
            .describeClientQuotas(ClientQuotaFilter.containsOnly(
                List.of(ClientQuotaFilterComponent.ofEntity("user", "user1")))
            ).entities().get();

        assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        assertTrue(quotas.containsKey("producer_byte_rate"));
        assertEquals(102400.0, quotas.get("producer_byte_rate"));
        assertTrue(quotas.containsKey("consumer_byte_rate"));
        assertEquals(102400.0, quotas.get("consumer_byte_rate"));
    }

    @Test
    void checkCustomQuotas() throws ExecutionException, InterruptedException {
        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
            .describeClientQuotas(ClientQuotaFilter.containsOnly(
                List.of(ClientQuotaFilterComponent.ofEntity("user", "user2")))
            ).entities().get();

        assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        assertTrue(quotas.containsKey("producer_byte_rate"));
        assertEquals(204800.0, quotas.get("producer_byte_rate"));
        assertTrue(quotas.containsKey("consumer_byte_rate"));
        assertEquals(409600.0, quotas.get("consumer_byte_rate"));
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

        client.toBlocking().exchange(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns3/resource-quotas").bearerAuth(token).body(rq3));

        // Force user sync to force the quota update
        userAsyncExecutors.forEach(UserAsyncExecutor::run);

        Map<ClientQuotaEntity, Map<String, Double>> mapQuota = getAdminClient()
            .describeClientQuotas(ClientQuotaFilter.containsOnly(
                List.of(ClientQuotaFilterComponent.ofEntity("user", "user3")))
            ).entities().get();

        assertEquals(1, mapQuota.entrySet().size());
        Map<String, Double> quotas = mapQuota.entrySet().stream().findFirst().get().getValue();
        assertTrue(quotas.containsKey("producer_byte_rate"));
        assertEquals(204800.0, quotas.get("producer_byte_rate"));
        assertTrue(quotas.containsKey("consumer_byte_rate"));
        assertEquals(409600.0, quotas.get("consumer_byte_rate"));
    }

    @Test
    void createAndUpdateUserForceTest() throws ExecutionException, InterruptedException {
        KafkaUserResetPassword response = client.toBlocking().retrieve(
            HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/users/user1/reset-password").bearerAuth(token),
            KafkaUserResetPassword.class);

        Map<String, UserScramCredentialsDescription> mapUser = getAdminClient()
            .describeUserScramCredentials(List.of("user1")).all().get();

        Assertions.assertNotNull(response.getSpec().getNewPassword());
        assertTrue(mapUser.containsKey("user1"));
        assertEquals(ScramMechanism.SCRAM_SHA_512, mapUser.get("user1").credentialInfos().get(0).mechanism());
        assertEquals(4096, mapUser.get("user1").credentialInfos().get(0).iterations());
    }

    @Test
    void updateUserFail_NotMatching() {
        HttpClientResponseException exception = assertThrows(HttpClientResponseException.class,
            () -> client.toBlocking().retrieve(
                HttpRequest.create(HttpMethod.POST, "/api/namespaces/ns1/users/user2/reset-password").bearerAuth(token),
                KafkaUserResetPassword.class));

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, exception.getStatus());
        assertEquals("Invalid user user2 : Doesn't belong to namespace ns1",
            exception.getResponse().getBody(Status.class).get().getDetails().getCauses().get(0));
    }
}
