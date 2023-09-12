package com.michelin.ns4kafka.models;

import io.micronaut.serde.annotation.Serdeable;
import lombok.*;

@Getter
@Setter
@Builder
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class KafkaUserResetPassword {
    private final String apiVersion = "v1";
    private final String kind = "KafkaUserResetPassword";
    private ObjectMeta metadata;
    private KafkaUserResetPasswordSpec spec;

    @Getter
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KafkaUserResetPasswordSpec {
        private String newPassword;
    }
}
