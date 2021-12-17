package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.KafkaUserResetPassword;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.services.UserService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Tag(name = "Users")
@Controller(value = "/api/namespaces/{namespace}/users")
public class UserController extends NamespacedResourceController {

    @Inject
    UserService userService;

    @Post("/reset-password")
    public HttpResponse<KafkaUserResetPassword> resetPassword(String namespace) throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = getNamespace(namespace);

        List<String> validationErrors = userService.validatePasswordReset(ns);
        if(!validationErrors.isEmpty()){
            throw new ResourceValidationException(validationErrors, "KafkaUserResetPassword", namespace);
        }
        //TODO QuotaManagement + UserAsyncExecutor
        userService.forceQuotas(ns);

        String password = userService.resetPassword(ns);

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
