package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

@Getter
@Setter
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class KafkaUserResetPassword {
    private final String apiVersion = "v1";
    private final String kind = "KafkaUserResetPassword";
    private ObjectMeta metadata;
    private KafkaUserResetPasswordSpec spec;

    @Getter
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KafkaUserResetPasswordSpec {
        private String newPassword;
    }
}
