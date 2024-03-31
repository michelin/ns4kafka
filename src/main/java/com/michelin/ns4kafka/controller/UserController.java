package com.michelin.ns4kafka.controller;

import static com.michelin.ns4kafka.util.FormatErrorUtils.invalidKafkaUser;
import static com.michelin.ns4kafka.util.enumation.Kind.KAFKA_USER_RESET_PASSWORD;

import com.michelin.ns4kafka.controller.generic.NamespacedResourceController;
import com.michelin.ns4kafka.model.KafkaUserResetPassword;
import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.Namespace;
import com.michelin.ns4kafka.service.executor.UserAsyncExecutor;
import com.michelin.ns4kafka.util.enumation.ApplyStatus;
import com.michelin.ns4kafka.util.exception.ResourceValidationException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Date;

/**
 * Controller to manage users.
 */
@Tag(name = "Users", description = "Manage the users.")
@Controller(value = "/api/namespaces/{namespace}/users")
public class UserController extends NamespacedResourceController {
    @Inject
    ApplicationContext applicationContext;

    /**
     * Reset a password.
     *
     * @param namespace The namespace
     * @param user      The user
     * @return The new password
     */
    @Post("/{user}/reset-password")
    public HttpResponse<KafkaUserResetPassword> resetPassword(String namespace, String user) {
        Namespace ns = getNamespace(namespace);

        if (!ns.getSpec().getKafkaUser().equals(user)) {
            throw new ResourceValidationException(KAFKA_USER_RESET_PASSWORD, user, invalidKafkaUser(user));
        }

        UserAsyncExecutor userAsyncExecutor =
            applicationContext.getBean(UserAsyncExecutor.class, Qualifiers.byName(ns.getMetadata().getCluster()));

        String password = userAsyncExecutor.resetPassword(ns.getSpec().getKafkaUser());

        KafkaUserResetPassword response = KafkaUserResetPassword.builder()
            .metadata(Metadata.builder()
                .name(ns.getSpec().getKafkaUser())
                .namespace(namespace)
                .cluster(ns.getMetadata().getCluster())
                .creationTimestamp(Date.from(Instant.now()))
                .build())
            .spec(KafkaUserResetPassword.KafkaUserResetPasswordSpec.builder()
                .newPassword(password)
                .build())
            .build();

        sendEventLog(response, ApplyStatus.changed, null, response.getSpec());
        return HttpResponse.ok(response);
    }
}
