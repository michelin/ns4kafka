package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.executors.KafkaAsyncExecutorConfig;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Singleton
public class UserService {

    @Inject
    ApplicationContext applicationContext;

    private final ScramCredentialInfo info = new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096);
    private final SecureRandom secureRandom = new SecureRandom();


    public String resetPassword(Namespace namespace) throws ExecutionException, InterruptedException, TimeoutException {
        byte[] randomBytes = new byte[48];
        secureRandom.nextBytes(randomBytes);
        String password = Base64.getEncoder().encodeToString(randomBytes);

        Admin admin = applicationContext.getBean(KafkaAsyncExecutorConfig.class, Qualifiers.byName(namespace.getMetadata().getCluster())).getAdminClient();
        UserScramCredentialUpsertion update = new UserScramCredentialUpsertion(namespace.getSpec().getKafkaUser(), info, password);

        admin.alterUserScramCredentials(List.of(update)).all().get(10, TimeUnit.SECONDS);
        return password;
    }

    public void forceQuotas(Namespace namespace) throws ExecutionException, InterruptedException, TimeoutException {
        Admin admin = applicationContext.getBean(KafkaAsyncExecutorConfig.class, Qualifiers.byName(namespace.getMetadata().getCluster())).getAdminClient();
        ClientQuotaEntity client = new ClientQuotaEntity(Map.of("user", namespace.getSpec().getKafkaUser()));
        ClientQuotaAlteration.Op producer_quota = new ClientQuotaAlteration.Op("producer_byte_rate", 102400.0);
        ClientQuotaAlteration.Op consumer_quota = new ClientQuotaAlteration.Op("consumer_byte_rate", 102400.0);
        ClientQuotaAlteration clientQuota = new ClientQuotaAlteration(client, List.of(producer_quota, consumer_quota));
        admin.alterClientQuotas(List.of(clientQuota)).all().get(10, TimeUnit.SECONDS);

    }
}
