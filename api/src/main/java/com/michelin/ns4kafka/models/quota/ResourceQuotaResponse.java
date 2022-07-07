package com.michelin.ns4kafka.models.quota;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Getter
@Builder
@Introspected
@NoArgsConstructor
@AllArgsConstructor
public class ResourceQuotaResponse {
    /**
     * API version
     */
    private final String apiVersion = "v1";

    /**
     * Resource kind
     */
    private final String kind = "ResourceQuotaResponse";

    /**
     * Resource specifications
     */
    @Valid
    @NotNull
    private ResourceQuotaResponseSpec spec;

    @Getter
    @Builder
    @ToString
    @Introspected
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ResourceQuotaResponseSpec {
        /**
         * The resource name
         */
        private ResourceQuota.ResourceQuotaSpecKey resourceName;

        /**
         * The currently used rate
         */
        private String used;

        /**
         * The quota limit
         */
        private String limit;
    }
}
