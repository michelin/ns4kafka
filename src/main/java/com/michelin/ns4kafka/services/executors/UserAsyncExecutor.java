package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import io.micronaut.context.annotation.EachBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@EachBean(KafkaAsyncExecutorConfig.class)
@Singleton
public class UserAsyncExecutor {

    private final KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig;
    private final AbstractUserSynchronizer userExecutor;

    @Inject
    NamespaceRepository namespaceRepository;

    public UserAsyncExecutor(KafkaAsyncExecutorConfig kafkaAsyncExecutorConfig) {
        this.kafkaAsyncExecutorConfig = kafkaAsyncExecutorConfig;
        switch (kafkaAsyncExecutorConfig.getProvider()) {
            case SELF_MANAGED:
                this.userExecutor = new Scram512UserSynchronizer(kafkaAsyncExecutorConfig.getAdminClient());
                break;
            case CONFLUENT_CLOUD:
            default:
                this.userExecutor = new UnimplementedUserSynchronizer();
                break;
        }
    }

    public void run() {
        if (this.kafkaAsyncExecutorConfig.isManageUsers() && this.userExecutor.canSynchronizeQuotas()) {
            synchronizeUsers();
        }

    }

    public void synchronizeUsers() {
        log.debug("Starting user collection for cluster {}", kafkaAsyncExecutorConfig.getName());
        // List user details from broker
        Map<String, Map<String, Double>> brokerUserQuotas = this.userExecutor.listQuotas();
        // List user details from ns4kafka
        Map<String, Map<String, Double>> ns4kafkaUserQuotas = collectNs4kafkaQuotas();

        // Compute toCreate, toDelete, and toUpdate lists
        Map<String, Map<String, Double>> toCreate = ns4kafkaUserQuotas.entrySet()
                .stream()
                .filter(entry -> !brokerUserQuotas.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Map<String, Double>> toDelete = brokerUserQuotas.entrySet()
                .stream()
                .filter(entry -> !ns4kafkaUserQuotas.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Map<String, Double>> toUpdate = ns4kafkaUserQuotas.entrySet()
                .stream()
                .filter(entry -> brokerUserQuotas.containsKey(entry.getKey()))
                .filter(entry -> entry.getValue().equals(brokerUserQuotas.get(entry.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (log.isDebugEnabled()) {
            log.debug("UserQuotas to create : " + toCreate.keySet().stream().collect(Collectors.joining(", ")));
            log.debug("UserQuotas to delete : " + toDelete.size());
            log.debug("UserQuotas to update : " + toUpdate.size());
        }

        createUserQuotas(toCreate);
        deleteUserQuotas(toDelete);
        createUserQuotas(toUpdate);

    }

    public String resetPassword(String user) {
        if (this.userExecutor.canResetPassword()) {
            return this.userExecutor.resetPassword(user);
        } else {
            throw new ResourceValidationException(
                    List.of("Password reset is not available with provider "+kafkaAsyncExecutorConfig.getProvider()),
                    "KafkaUserResetPassword",
                    user);
        }
    }

    private Map<String, Map<String, Double>> collectNs4kafkaQuotas() {
        // TODO fetch data from QuotaRepository when available
        return namespaceRepository.findAllForCluster(this.kafkaAsyncExecutorConfig.getName())
                .stream()
                .map(namespace -> Map.entry(
                        namespace.getSpec().getKafkaUser(),
                        Map.of(
                                "producer_byte_rate", 102400.0,
                                "consumer_byte_rate", 102400.0)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void createUserQuotas(Map<String, Map<String, Double>> toCreate) {
        toCreate.forEach(this.userExecutor::applyQuotas);
    }

    private void deleteUserQuotas(Map<String, Map<String, Double>> toDelete) {
        // Not deleting quotas that could impact users not managed by ns4kafka.
    }

    interface AbstractUserSynchronizer {
        boolean canSynchronizeQuotas();

        boolean canResetPassword();

        String resetPassword(String user);

        void applyQuotas(String user, Map<String, Double> quotas);

        Map<String, Map<String, Double>> listQuotas();
    }

    static class Scram512UserSynchronizer implements AbstractUserSynchronizer {

        private Admin admin;

        private final ScramCredentialInfo info = new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096);
        private final SecureRandom secureRandom = new SecureRandom();

        public Scram512UserSynchronizer(Admin admin) {
            this.admin = admin;
        }

        @Override
        public boolean canSynchronizeQuotas() {
            return true;
        }

        @Override
        public boolean canResetPassword() {
            return true;
        }

        @Override
        public String resetPassword(String user) {
            byte[] randomBytes = new byte[48];
            secureRandom.nextBytes(randomBytes);
            String password = Base64.getEncoder().encodeToString(randomBytes);
            UserScramCredentialUpsertion update = new UserScramCredentialUpsertion(user, info, password);
            try {
                admin.alterUserScramCredentials(List.of(update)).all().get(10, TimeUnit.SECONDS);
                log.info("Success resetting password for user {}", user);
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return password;
        }

        @Override
        public Map<String, Map<String, Double>> listQuotas() {
            ClientQuotaFilter filter = ClientQuotaFilter.containsOnly(List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)));
            try {
                return admin.describeClientQuotas(filter).entities().get(10, TimeUnit.SECONDS)
                        .entrySet()
                        .stream()
                        .map(entry -> Map.entry(entry.getKey().entries().get(ClientQuotaEntity.USER), entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public void applyQuotas(String user, Map<String, Double> quotas) {
            ClientQuotaEntity client = new ClientQuotaEntity(Map.of("user", user));
            ClientQuotaAlteration.Op producer_quota = new ClientQuotaAlteration.Op("producer_byte_rate", 102400.0);
            ClientQuotaAlteration.Op consumer_quota = new ClientQuotaAlteration.Op("consumer_byte_rate", 102400.0);
            ClientQuotaAlteration clientQuota = new ClientQuotaAlteration(client, List.of(producer_quota, consumer_quota));
            try {
                admin.alterClientQuotas(List.of(clientQuota)).all().get(10, TimeUnit.SECONDS);
                log.info("Success applying quotas {} for user {}", quotas, user);
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error(String.format("Error while applying quotas for user %s", user), e);

            }

        }
    }

    static class UnimplementedUserSynchronizer implements AbstractUserSynchronizer {

        private final UnsupportedOperationException exception = new UnsupportedOperationException("This cluster provider doesn't support User operations.");

        @Override
        public boolean canSynchronizeQuotas() {
            return false;
        }

        @Override
        public boolean canResetPassword() {
            return false;
        }

        @Override
        public String resetPassword(String user) {
            throw exception;
        }

        @Override
        public void applyQuotas(String user, Map<String, Double> quotas) {
            throw exception;
        }

        @Override
        public Map<String, Map<String, Double>> listQuotas() {
            throw exception;
        }
    }
}
