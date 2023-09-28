package com.michelin.ns4kafka.models.connect.cluster;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import lombok.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

@Data
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class ConnectCluster {
    private final String apiVersion = "v1";
    private final String kind = "ConnectCluster";

    @Valid
    @NotNull
    private ObjectMeta metadata;

    @Valid
    @NotNull
    private ConnectClusterSpec spec;

    @Data
    @Builder
    @Introspected
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectClusterSpec {
        /**
         * Gets or sets the Kafka Connect Cluster url.
         */
        @NotNull
        String url;

        /**
         * Gets or sets the authentication username.
         */
        String username;

        /**
         * Gets or sets the authentication password.
         */
        String password;

        /**
         * Gets the Kafka Connect status
         */
        @EqualsAndHashCode.Exclude
        Status status;

        /**
         * Gets the Kafka Connect status context message
         */
        @EqualsAndHashCode.Exclude
        String statusMessage;

        /**
         * Gets or sets the aes256 key.
         */
        String aes256Key;

        /**
         * Gets or sets the aes256 salt.
         */
        String aes256Salt;

        /**
         * Gets or sets the aes256 key.
         */
        String aes256Format;
    }

    public enum Status {
        HEALTHY,
        IDLE
    }
}
