package com.michelin.ns4kafka.model;

import static com.michelin.ns4kafka.util.enumation.Kind.KAFKA_USER_RESET_PASSWORD;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Kafka user reset password.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class KafkaUserResetPassword extends MetadataResource {
    private KafkaUserResetPasswordSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public KafkaUserResetPassword(Metadata metadata, KafkaUserResetPasswordSpec spec) {
        super("v1", KAFKA_USER_RESET_PASSWORD, metadata);
        this.spec = spec;
    }

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
