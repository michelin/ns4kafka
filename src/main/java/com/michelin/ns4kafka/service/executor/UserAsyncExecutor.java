/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.ns4kafka.service.executor;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidResetPasswordProvider;
import static com.michelin.ns4kafka.util.enumation.Kind.KAFKA_USER_RESET_PASSWORD;

import com.michelin.ns4kafka.model.quota.ResourceQuota;
import com.michelin.ns4kafka.property.ManagedClusterProperties;
import com.michelin.ns4kafka.repository.NamespaceRepository;
import com.michelin.ns4kafka.repository.ResourceQuotaRepository;
import com.michelin.ns4kafka.repository.kafka.KafkaStoreException;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.annotation.EachBean;
import jakarta.inject.Singleton;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

/** User executor. */
@Slf4j
@EachBean(ManagedClusterProperties.class)
@Singleton
public class UserAsyncExecutor {
    public static final double BYTE_RATE_DEFAULT_VALUE = 102400.0;
    private static final String USER_QUOTA_PREFIX = "user/";
    private final ManagedClusterProperties managedClusterProperties;
    private final NamespaceRepository namespaceRepository;
    private final ResourceQuotaRepository quotaRepository;
    private final AbstractUserSynchronizer userExecutor;

    /**
     * Constructor.
     *
     * @param managedClusterProperties The managed cluster properties
     * @param namespaceRepository The namespace repository
     * @param quotaRepository The resource quota repository
     */
    public UserAsyncExecutor(
            ManagedClusterProperties managedClusterProperties,
            NamespaceRepository namespaceRepository,
            ResourceQuotaRepository quotaRepository) {
        this.managedClusterProperties = managedClusterProperties;
        this.namespaceRepository = namespaceRepository;
        this.quotaRepository = quotaRepository;

        if (Objects.requireNonNull(managedClusterProperties.getProvider())
                == ManagedClusterProperties.KafkaProvider.SELF_MANAGED) {
            this.userExecutor = new Scram512UserSynchronizer(managedClusterProperties);
        } else {
            this.userExecutor = new UnimplementedUserSynchronizer();
        }
    }

    /** Run the user synchronization. */
    public void run() {
        if (managedClusterProperties.isManageUsers() && userExecutor.canSynchronizeQuotas()) {
            synchronizeUsers();
        }
    }

    /** Start the user synchronization. */
    public void synchronizeUsers() {
        log.debug("Starting user collection for cluster {}", managedClusterProperties.getName());

        try {
            Map<String, Map<String, Double>> brokerUserQuotas = userExecutor.listQuotas();
            Map<String, Map<String, Double>> ns4kafkaUserQuotas = collectNs4KafkaQuotas();

            Map<String, Map<String, Double>> toCreate = ns4kafkaUserQuotas.entrySet().stream()
                    .filter(entry -> !brokerUserQuotas.containsKey(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Map<String, Map<String, Double>> toUpdate = ns4kafkaUserQuotas.entrySet().stream()
                    .filter(entry -> brokerUserQuotas.containsKey(entry.getKey()))
                    .filter(entry -> !entry.getValue().isEmpty()
                            && !entry.getValue().equals(brokerUserQuotas.get(entry.getKey())))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!toCreate.isEmpty()) {
                log.debug("User quota(s) to create : {}", String.join(", ", toCreate.keySet()));
            }

            if (!toUpdate.isEmpty()) {
                log.debug("User quota(s) to update : {}", String.join(", ", toUpdate.keySet()));
            }

            createUserQuotas(toCreate);
            createUserQuotas(toUpdate);
        } catch (ExecutionException | TimeoutException | CancellationException | KafkaStoreException e) {
            log.error("An error occurred during the user synchronization", e);
        } catch (InterruptedException e) {
            log.error("Thread interrupted during the user synchronization", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Reset the password of a given user.
     *
     * @param user The user
     * @return The new password
     * @throws ExecutionException An error occurred during the execution of the password reset
     * @throws TimeoutException An operation timed out while resetting the password
     * @throws InterruptedException The thread was interrupted while waiting for the password reset
     */
    public String resetPassword(String user) throws ExecutionException, InterruptedException, TimeoutException {
        if (userExecutor.canResetPassword()) {
            return userExecutor.resetPassword(user);
        } else {
            throw new ResourceValidationException(
                    KAFKA_USER_RESET_PASSWORD,
                    user,
                    invalidResetPasswordProvider(managedClusterProperties.getProvider()));
        }
    }

    private Map<String, Map<String, Double>> collectNs4KafkaQuotas() {
        return namespaceRepository.findAllForCluster(managedClusterProperties.getName()).stream()
                .map(namespace -> {
                    Optional<ResourceQuota> quota = quotaRepository.findForNamespace(
                            namespace.getMetadata().getName());
                    Map<String, Double> userQuota = new HashMap<>();

                    quota.ifPresent(resourceQuota -> resourceQuota.getSpec().entrySet().stream()
                            .filter(q -> q.getKey().startsWith(USER_QUOTA_PREFIX))
                            .forEach(q -> userQuota.put(
                                    q.getKey().replace(USER_QUOTA_PREFIX, ""), Double.parseDouble(q.getValue()))));

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

        String resetPassword(String user) throws ExecutionException, InterruptedException, TimeoutException;

        void applyQuotas(String user, Map<String, Double> quotas);

        Map<String, Map<String, Double>> listQuotas() throws ExecutionException, InterruptedException, TimeoutException;
    }

    static class Scram512UserSynchronizer implements AbstractUserSynchronizer {
        private final ScramCredentialInfo info = new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096);
        private final SecureRandom secureRandom = new SecureRandom();
        private final ManagedClusterProperties managedClusterProperties;

        public Scram512UserSynchronizer(ManagedClusterProperties managedClusterProperties) {
            this.managedClusterProperties = managedClusterProperties;
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
        public String resetPassword(String user) throws ExecutionException, InterruptedException, TimeoutException {
            byte[] randomBytes = new byte[48];
            secureRandom.nextBytes(randomBytes);
            String password = Base64.getEncoder().encodeToString(randomBytes);
            UserScramCredentialUpsertion update = new UserScramCredentialUpsertion(user, info, password);

            managedClusterProperties
                    .getAdminClient()
                    .alterUserScramCredentials(List.of(update))
                    .all()
                    .get(
                            managedClusterProperties.getTimeout().getUser().getAlterScramCredentials(),
                            TimeUnit.MILLISECONDS);

            log.info("Success resetting password for user {}", user);

            return password;
        }

        @Override
        public Map<String, Map<String, Double>> listQuotas()
                throws ExecutionException, InterruptedException, TimeoutException {
            ClientQuotaFilter filter = ClientQuotaFilter.containsOnly(
                    List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER)));

            return managedClusterProperties
                    .getAdminClient()
                    .describeClientQuotas(filter)
                    .entities()
                    .get(managedClusterProperties.getTimeout().getUser().getDescribeQuotas(), TimeUnit.MILLISECONDS)
                    .entrySet()
                    .stream()
                    .map(entry -> Map.entry(entry.getKey().entries().get(ClientQuotaEntity.USER), entry.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public void applyQuotas(String user, Map<String, Double> quotas) {
            ClientQuotaEntity client = new ClientQuotaEntity(Map.of("user", user));

            ClientQuotaAlteration.Op producerQuota = new ClientQuotaAlteration.Op(
                    "producer_byte_rate", quotas.getOrDefault("producer_byte_rate", BYTE_RATE_DEFAULT_VALUE));

            ClientQuotaAlteration.Op consumerQuota = new ClientQuotaAlteration.Op(
                    "consumer_byte_rate", quotas.getOrDefault("consumer_byte_rate", BYTE_RATE_DEFAULT_VALUE));

            ClientQuotaAlteration clientQuota =
                    new ClientQuotaAlteration(client, List.of(producerQuota, consumerQuota));

            try {
                managedClusterProperties
                        .getAdminClient()
                        .alterClientQuotas(List.of(clientQuota))
                        .all()
                        .get(managedClusterProperties.getTimeout().getUser().getAlterQuotas(), TimeUnit.MILLISECONDS);
                log.info("Success applying quotas {} for user {}", clientQuota.ops(), user);
            } catch (InterruptedException e) {
                log.error("Error", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error while applying quotas for user %s".formatted(user), e);
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
