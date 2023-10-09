package com.michelin.ns4kafka.services.executors;

import com.michelin.ns4kafka.models.quota.ResourceQuota;
import com.michelin.ns4kafka.properties.ManagedClusterProperties;
import com.michelin.ns4kafka.repositories.NamespaceRepository;
import com.michelin.ns4kafka.repositories.ResourceQuotaRepository;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

/**
 * User executor.
 */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class UserAsyncExecutor {
    public static final double BYTE_RATE_DEFAULT_VALUE = 102400.0;

    private static final String USER_QUOTA_PREFIX = "user/";

    private final ManagedClusterProperties managedClusterProperties;

    private final AbstractUserSynchronizer userExecutor;

    @Inject
    NamespaceRepository namespaceRepository;

    @Inject
    ResourceQuotaRepository quotaRepository;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     */
    public UserAsyncExecutor(ManagedClusterProperties managedClusterProperties) {
        this.managedClusterProperties = managedClusterProperties;
        if (Objects.requireNonNull(managedClusterProperties.getProvider())
            == ManagedClusterProperties.KafkaProvider.SELF_MANAGED) {
            this.userExecutor = new Scram512UserSynchronizer(managedClusterProperties.getAdminClient());
        } else {
            this.userExecutor = new UnimplementedUserSynchronizer();
        }
    }

    /**
     * Run the user synchronization.
     */
    public void run() {
        if (this.managedClusterProperties.isManageUsers() && this.userExecutor.canSynchronizeQuotas()) {
            synchronizeUsers();
        }

    }

    /**
     * Start the user synchronization.
     */
    public void synchronizeUsers() {
        log.debug("Starting user collection for cluster {}", managedClusterProperties.getName());
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
            .filter(
                entry -> !entry.getValue().isEmpty() && !entry.getValue().equals(brokerUserQuotas.get(entry.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (log.isDebugEnabled()) {
            log.debug("UserQuotas to create : " + String.join(", ", toCreate.keySet()));
            log.debug("UserQuotas to delete : " + toDelete.size());
            log.debug("UserQuotas to update : " + toUpdate.size());
        }

        createUserQuotas(toCreate);
        createUserQuotas(toUpdate);
    }

    /**
     * Reset the password of a given user.
     *
     * @param user The user
     * @return The new password
     */
    public String resetPassword(String user) {
        if (this.userExecutor.canResetPassword()) {
            return this.userExecutor.resetPassword(user);
        } else {
            throw new ResourceValidationException(
                List.of("Password reset is not available with provider " + managedClusterProperties.getProvider()),
                "KafkaUserResetPassword",
                user);
        }
    }

    private Map<String, Map<String, Double>> collectNs4kafkaQuotas() {
        return namespaceRepository.findAllForCluster(this.managedClusterProperties.getName())
            .stream()
            .map(namespace -> {
                Optional<ResourceQuota> quota = quotaRepository.findForNamespace(namespace.getMetadata().getName());
                Map<String, Double> userQuota = new HashMap<>();

                quota.ifPresent(resourceQuota -> resourceQuota.getSpec().entrySet()
                    .stream()
                    .filter(q -> q.getKey().startsWith(USER_QUOTA_PREFIX))
                    .forEach(q -> userQuota.put(
                        q.getKey().replace(USER_QUOTA_PREFIX, ""),
                        Double.parseDouble(q.getValue()))));

                return Map.entry(namespace.getSpec().getKafkaUser(), userQuota);
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void createUserQuotas(Map<String, Map<String, Double>> toCreate) {
        toCreate.forEach(this.userExecutor::applyQuotas);
    }

    interface AbstractUserSynchronizer {
        boolean canSynchronizeQuotas();

        boolean canResetPassword();

        String resetPassword(String user);

        void applyQuotas(String user, Map<String, Double> quotas);

        Map<String, Map<String, Double>> listQuotas();
    }

    static class Scram512UserSynchronizer implements AbstractUserSynchronizer {

        private final ScramCredentialInfo info = new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096);
        private final SecureRandom secureRandom = new SecureRandom();
        private final Admin admin;

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
            ClientQuotaFilter filter = ClientQuotaFilter.containsOnly(
                List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)));
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
            ClientQuotaAlteration.Op producerQuota = new ClientQuotaAlteration.Op("producer_byte_rate",
                quotas.getOrDefault("producer_byte_rate", BYTE_RATE_DEFAULT_VALUE));
            ClientQuotaAlteration.Op consumerQuota = new ClientQuotaAlteration.Op("consumer_byte_rate",
                quotas.getOrDefault("consumer_byte_rate", BYTE_RATE_DEFAULT_VALUE));
            ClientQuotaAlteration clientQuota =
                new ClientQuotaAlteration(client, List.of(producerQuota, consumerQuota));
            try {
                admin.alterClientQuotas(List.of(clientQuota)).all().get(10, TimeUnit.SECONDS);
                log.info("Success applying quotas {} for user {}", clientQuota.ops(), user);
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error(String.format("Error while applying quotas for user %s", user), e);

            }

        }
    }

    static class UnimplementedUserSynchronizer implements AbstractUserSynchronizer {

        private final UnsupportedOperationException exception =
            new UnsupportedOperationException("This cluster provider doesn't support User operations.");

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
