package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.controllers.generic.NamespacedResourceController;
import com.michelin.ns4kafka.models.KafkaUserResetPassword;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.executors.UserAsyncExecutor;
import com.michelin.ns4kafka.utils.enums.ApplyStatus;
import com.michelin.ns4kafka.utils.exceptions.ResourceValidationException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.Date;
import java.util.List;

@Tag(name = "Users", description = "Manage the users.")
@Controller(value = "/api/namespaces/{namespace}/users")
public class UserController extends NamespacedResourceController {
    @Inject
    ApplicationContext applicationContext;

    /**
     * Reset a password
     * @param namespace The namespace
     * @param user The user
     * @return The new password
     */
    @Post("/{user}/reset-password")
    public HttpResponse<KafkaUserResetPassword> resetPassword(String namespace, String user) {
        Namespace ns = getNamespace(namespace);

        if(!ns.getSpec().getKafkaUser().equals(user)){
            throw new ResourceValidationException(List.of(String.format("Invalid user %s : Doesn't belong to namespace %s", user, namespace)), "KafkaUserResetPassword", user);
        }

        UserAsyncExecutor userAsyncExecutor = applicationContext.getBean(UserAsyncExecutor.class, Qualifiers.byName(ns.getMetadata().getCluster()));

        String password = userAsyncExecutor.resetPassword(ns.getSpec().getKafkaUser());

        KafkaUserResetPassword response = KafkaUserResetPassword.builder()
                .metadata(ObjectMeta.builder()
                        .name(ns.getSpec().getKafkaUser())
                        .namespace(namespace)
                        .cluster(ns.getMetadata().getCluster())
                        .creationTimestamp(Date.from(Instant.now()))
                        .build())
                .spec(KafkaUserResetPassword.KafkaUserResetPasswordSpec.builder()
                        .newPassword(password)
                        .build())
                .build();
        sendEventLog("KafkaUserResetPassword", response.getMetadata(), ApplyStatus.changed, null, response.getSpec());
        return HttpResponse.ok(response);
    }
}
