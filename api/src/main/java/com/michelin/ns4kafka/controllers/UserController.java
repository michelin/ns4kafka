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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Tag(name = "Users")
@Controller(value = "/api/namespaces/{namespace}/users")
public class UserController extends NamespacedResourceController {

    @Inject
    UserService userService;

    @Post("/reset-password{?dryrun}")
    public HttpResponse<KafkaUserResetPassword> resetPassword(String namespace) throws ExecutionException, InterruptedException, TimeoutException {
        Namespace ns = getNamespace(namespace);

        String password = userService.resetPassword(ns);
        return HttpResponse.ok(KafkaUserResetPassword.builder()
                .metadata(ObjectMeta.builder()
                        .name(ns.getSpec().getKafkaUser())
                        .namespace(namespace)
                        .cluster(ns.getMetadata().getCluster())
                        .build())
                .spec(KafkaUserResetPassword.KafkaUserResetPasswordSpec.builder()
                        .newPassword(password)
                        .build())
                .build());
    }
}
