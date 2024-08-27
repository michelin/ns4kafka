package com.michelin.ns4kafka.service.executor;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidResetPasswordProvider;
import static com.michelin.ns4kafka.util.enumation.Kind.KAFKA_USER_RESET_PASSWORD;

import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.ResourceQuotaRepository;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import java.util.Properties;

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
        if (this.managedClusterProperties.isManageUsers() && userExecutor.canSynchronizeQuotas()) {
            synchronizeUsers();
        }

    }

    /**
     * Start the user synchronization.
     */
    public void synchronizeUsers() {
        log.debug("Starting user collection for cluster {}", managedClusterProperties.getName());

        // List user details from broker
        Map<String, Map<String, Double>> brokerUserQuotas = userExecutor.listQuotas();
        // List user details from ns4kafka
        Map<String, Map<String, Double>> ns4kafkaUserQuotas = collectNs4kafkaQuotas();

        Map<String, Map<String, Double>> toCreate = ns4kafkaUserQuotas.entrySet()
            .stream()
            .filter(entry -> !brokerUserQuotas.containsKey(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, Map<String, Double>> toUpdate = ns4kafkaUserQuotas.entrySet()
            .stream()
            .filter(entry -> brokerUserQuotas.containsKey(entry.getKey()))
            .filter(
                entry -> !entry.getValue().isEmpty() && !entry.getValue().equals(brokerUserQuotas.get(entry.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!toCreate.isEmpty()) {
            log.debug("User quota(s) to create : " + String.join(", ", toCreate.keySet()));
        }

        if (!toUpdate.isEmpty()) {
            log.debug("User quota(s) to update : " + String.join(", ", toUpdate.keySet()));
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
        if (userExecutor.canResetPassword()) {
            return userExecutor.resetPassword(user);
        } else {
            throw new ResourceValidationException(KAFKA_USER_RESET_PASSWORD, user,
                invalidResetPasswordProvider(managedClusterProperties.getProvider()));
        }
    }

    /**
     * Set the password of a given user.
     *
     * @param user The user
     * @param password The new password
     */
    public void setPassword(String user, String password) {
        if (userExecutor.canResetPassword()) {
            userExecutor.setPassword(user, password);
        } else {
            throw new ResourceValidationException(KAFKA_USER_RESET_PASSWORD, user,
                invalidResetPasswordProvider(managedClusterProperties.getProvider()));
        }
    }

    /**
     * Check if the password matches for given user.
     *
     * @param user The user
     * @param password The password to test
     * @return true if given password matches current one
     */
    public boolean checkPassword(String user, String password) {
        return userExecutor.checkPassword(user, password, managedClusterProperties.getConfig());
    }

    private Map<String, Map<String, Double>> collectNs4kafkaQuotas() {
        return namespaceRepository.findAllForCluster(managedClusterProperties.getName())
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
        toCreate.forEach(userExecutor::applyQuotas);
    }

    interface AbstractUserSynchronizer {
        boolean canSynchronizeQuotas();

        boolean canResetPassword();

        String resetPassword(String user);
        void setPassword(String user, String password);
        boolean checkPassword(String user, String password, Properties config);

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
        public void setPassword(String user, String password) {
            UserScramCredentialUpsertion update = new UserScramCredentialUpsertion(user, info, password);
            try {
                admin.alterUserScramCredentials(List.of(update)).all().get(10, TimeUnit.SECONDS);
                log.info("Success setting password for user {}", user);
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean checkPassword(String user, String password, Properties config) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap.servers"));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            // Configure SCRAM-SHA256 authentication
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, config.get("security.protocol"));
            // TODO : what if it's not SCRAM ??
            props.put(SaslConfigs.SASL_MECHANISM, config.get("sasl.mechanism"));
            props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
                + user + "\" password=\"" + password + "\";");
            // props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000); // Reduce the delivery timeout to avoid blocking caller too long

            log.debug("Configuring connection to " + config.get("bootstrap.servers"));
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            try {
                // Create a Kafka producer with the configured properties
                // List<PartitionInfo> partitionInfoList =
                log.debug("Connecting ...");
                Object dummy = producer.partitionsFor("__consumer_offsets"); // this topic should always exist

                // If the authentication is successful, the password is correct
                log.debug("Access to topic -> Password is correct");
                return true;

            } catch (org.apache.kafka.common.errors.TopicAuthorizationException e) {
                // not allowed to access to topic, but if broker says this, it means authent itself was successful
                log.debug("Topic forbidden, but password is correct");
                return true;
            } catch (org.apache.kafka.common.errors.SaslAuthenticationException e) {
                log.debug("Authent failed -> Password is incorrect");
                return false;
            } catch (Exception e) {
                // consider any other exception as a failed authent.
                log.debug("other exception {} -> Password is incorrect", e);
                return false;
            } finally {
                // Close the producer
                producer.close();
            }
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
        public void setPassword(String user, String password) {
            throw exception;
        }
        @Override
        public boolean checkPassword(String user, String password, Properties config) {
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
