package com.michelin.ns4kafka.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

@Introspected
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class KafkaUserResetPassword {
    private final String apiVersion = "v1";
    private final String kind = "KafkaUserResetPassword";

    private ObjectMeta metadata;

    private KafkaUserResetPasswordSpec spec;

    @Introspected
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class KafkaUserResetPasswordSpec {
        private String newPassword;
    }
}
