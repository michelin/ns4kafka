package com.michelin.ns4kafka.models;

import static com.michelin.ns4kafka.models.Kind.KAFKA_USER;

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
    private final String apiVersion = "v1";
    private final String kind = KAFKA_USER;
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
