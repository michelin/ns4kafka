package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Kafka user reset password.
 */
@Getter
@Setter
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class KafkaUserResetPassword {
    private static final String apiVersion = "v1";
    public static final String kind = "KafkaUserResetPassword";

    private ObjectMeta metadata;
    private KafkaUserResetPasswordSpec spec;

    /**
     * Kafka user reset password spec.
     */
    @Getter
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KafkaUserResetPasswordSpec {
        private String newPassword;
    }
}
