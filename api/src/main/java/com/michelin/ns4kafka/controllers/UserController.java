package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.KafkaUserResetPassword;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.executors.UserAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Date;

@Tag(name = "Users")
@Controller(value = "/api/namespaces/{namespace}/users")
public class UserController extends NamespacedResourceController {

    @Inject
    ApplicationContext applicationContext;

    // test flag for testing only, don't use it.
    @Post("/reset-password")
    public HttpResponse<KafkaUserResetPassword> resetPassword(String namespace) {
        Namespace ns = getNamespace(namespace);

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
