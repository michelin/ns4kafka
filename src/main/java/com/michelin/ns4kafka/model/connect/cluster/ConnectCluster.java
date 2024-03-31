package com.michelin.ns4kafka.model.connect.cluster;

import static com.michelin.ns4kafka.util.enumation.Kind.CONNECT_CLUSTER;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Kafka Connect Cluster.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class ConnectCluster extends MetadataResource {
    @Valid
    @NotNull
    private ConnectClusterSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public ConnectCluster(Metadata metadata, ConnectClusterSpec spec) {
        super("v1", CONNECT_CLUSTER, metadata);
        this.spec = spec;
    }

    /**
     * Kafka Connect status.
     */
    public enum Status {
        HEALTHY,
        IDLE
    }

    /**
     * Kafka Connect Cluster specification.
     */
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
         * Gets the Kafka Connect status.
         */
        @EqualsAndHashCode.Exclude
        Status status;

        /**
         * Gets the Kafka Connect status context message.
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
}
