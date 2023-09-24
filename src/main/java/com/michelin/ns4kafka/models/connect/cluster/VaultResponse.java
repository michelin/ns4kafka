package com.michelin.ns4kafka.models.connect.cluster;

import com.michelin.ns4kafka.models.ObjectMeta;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.serde.annotation.Serdeable;
import lombok.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * Represents the Kafka Connect Cluster Vault Response.
 */
@Data
@Builder
@Serdeable
@NoArgsConstructor
@AllArgsConstructor
public class VaultResponse {
    private final String apiVersion = "v1";
    private final String kind = "VaultResponse";

    /**
     * The object metadata
     */
    @Valid
    @NotNull
    private ObjectMeta metadata;

    /**
     * The vault resource spec.
     */
    @Valid
    @NotNull
    private VaultResponseSpec spec;

    /**
     * Represents the vault response specification.
     */
    @Data
    @Builder
    @Serdeable
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VaultResponseSpec {
        /**
         * The clear text to encrypt.
         */
        private String clearText;

        /**
         * The encrypted text.
         */
        private String encrypted;
    }
}
