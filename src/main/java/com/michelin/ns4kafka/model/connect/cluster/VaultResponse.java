package com.michelin.ns4kafka.model.connect.cluster;

import static com.michelin.ns4kafka.util.enumation.Kind.VAULT_RESPONSE;

import com.michelin.ns4kafka.model.Metadata;
import com.michelin.ns4kafka.model.MetadataResource;
import io.micronaut.core.annotation.Introspected;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Represents the Kafka Connect Cluster Vault Response.
 */
@Data
@Introspected
@EqualsAndHashCode(callSuper = true)
public class VaultResponse extends MetadataResource {
    /**
     * The vault resource spec.
     */
    @Valid
    @NotNull
    private VaultResponseSpec spec;

    /**
     * Constructor.
     *
     * @param metadata The metadata
     * @param spec     The spec
     */
    @Builder
    public VaultResponse(Metadata metadata, VaultResponseSpec spec) {
        super("v1", VAULT_RESPONSE, metadata);
        this.spec = spec;
    }

    /**
     * Represents the vault response specification.
     */
    @Getter
    @Builder
    @ToString
    @Introspected
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
