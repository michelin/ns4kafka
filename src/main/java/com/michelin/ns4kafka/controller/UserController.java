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
package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidKafkaUser;
import static com.michelin.ns4kafka.util.enumation.Kind.KAFKA_USER_API_KEY;
import static com.michelin.ns4kafka.util.enumation.Kind.KAFKA_USER_RESET_PASSWORD;
import static io.micronaut.core.util.StringUtils.EMPTY_STRING;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.AuditLog;
import com.michelin.ns4kafka.model.KafkaUserResetPassword;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.model.Resource;
import com.michelin.ns4kafka.service.NamespaceService;
import com.michelin.ns4kafka.service.client.confluent.ConfluentCloudClient;
import com.michelin.ns4kafka.service.executor.UserAsyncExecutor;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.annotation.Status;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.utils.SecurityService;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Controller to manage users. */
@Tag(name = "Users", description = "Manage the users.")
@Controller(value = "/api/namespaces/{namespace}/users")
@ExecuteOn(TaskExecutors.IO)
public class UserController extends NamespacedResourceController {
    private final ConfluentCloudClient confluentCloudClient;
    private final ApplicationContext applicationContext;

    /**
     * Constructor.
     *
     * @param confluentCloudClient The Confluent Cloud client
     * @param applicationContext The application context
     * @param namespaceService The namespace service
     * @param securityService The security service
     * @param applicationEventPublisher The application event publisher
     */
    protected UserController(
            ConfluentCloudClient confluentCloudClient,
            ApplicationContext applicationContext,
            NamespaceService namespaceService,
            SecurityService securityService,
            ApplicationEventPublisher<AuditLog> applicationEventPublisher) {
        super(namespaceService, securityService, applicationEventPublisher);
        this.confluentCloudClient = confluentCloudClient;
        this.applicationContext = applicationContext;
    }

    /**
     * Delete a Kafka API key belonging to the namespace's service account.
     *
     * @param namespace The namespace
     * @param user The service account ID
     * @param apiKey The API key ID
     * @param dryrun Whether to validate ownership without deleting the key
     * @return An empty response on success
     */
    @Delete("/{user}/api-keys")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> deleteApiKey(
            String namespace,
            String user,
            @QueryValue String apiKey,
            @QueryValue(defaultValue = "false") boolean dryrun) {
        Namespace ns = getNamespace(namespace);
        if (!user.equals(ns.getSpec().getKafkaUser())) {
            throw new ResourceValidationException(KAFKA_USER_API_KEY, user, invalidKafkaUser(user));
        }

        String cluster = ns.getMetadata().getCluster();
        if (!Boolean.TRUE.equals(
                confluentCloudClient.deleteApiKey(cluster, user, apiKey, dryrun).block())) {
            return HttpResponse.notFound();
        }
        if (!dryrun) {
            Resource.Metadata metadata = Resource.Metadata.builder()
                    .name(apiKey)
                    .namespace(namespace)
                    .cluster(cluster)
                    .build();
            sendEventLog(
                    new Resource("v1", KAFKA_USER_API_KEY, metadata), ApplyStatus.DELETED, null, null, EMPTY_STRING);
        }
        return HttpResponse.noContent();
    }

    /**
     * Reset a user's password.
     *
     * @param namespace The namespace in which the user exists
     * @param user The name of the user whose password is to be reset
     * @return The new password
     * @throws ExecutionException An error occurred during the execution of the password reset
     * @throws TimeoutException An operation timed out while resetting the password
     * @throws InterruptedException The thread was interrupted while waiting for the password reset
     */
    @Post("/{user}/reset-password")
    public HttpResponse<KafkaUserResetPassword> resetPassword(String namespace, String user)
            throws ExecutionException, TimeoutException, InterruptedException {
        Namespace ns = getNamespace(namespace);

        if (!ns.getSpec().getKafkaUser().equals(user)) {
            throw new ResourceValidationException(KAFKA_USER_RESET_PASSWORD, user, invalidKafkaUser(user));
        }

        UserAsyncExecutor userAsyncExecutor = applicationContext.getBean(
                UserAsyncExecutor.class, Qualifiers.byName(ns.getMetadata().getCluster()));

        String password = userAsyncExecutor.resetPassword(ns.getSpec().getKafkaUser());

        KafkaUserResetPassword response = KafkaUserResetPassword.builder()
                .metadata(Resource.Metadata.builder()
                        .name(ns.getSpec().getKafkaUser())
                        .namespace(namespace)
                        .cluster(ns.getMetadata().getCluster())
                        .creationTimestamp(Date.from(Instant.now()))
                        .build())
                .spec(KafkaUserResetPassword.KafkaUserResetPasswordSpec.builder()
                        .newPassword(password)
                        .build())
                .build();

        sendEventLog(response, ApplyStatus.CHANGED, null, response.getSpec(), EMPTY_STRING);

        return HttpResponse.ok(response);
    }
}
